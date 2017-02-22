import json
import logging
import os
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from enum import Enum
from queue import Queue, Empty
from typing import Any, Iterable, Tuple, Optional, List
import boto3
import pymongo
import tqdm
from pymongo import MongoClient
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from joblib import Parallel, delayed
# regex


integer_request_id_regex = re.compile(r"\d{10}")
date_regex = re.compile(r"\d\d\d\d-\d\d-\d\d( 00:00:00)?")


class CrawlStatus(Enum):
    CRAWLED = 1
    REQUESTED = 0


class CommitBeforeParkingException(Exception):
    pass


class ConcurrentParkingException(Exception):
    pass


class MissingDataPart(Exception):
    pass


def serialize_data_id(data_id) -> str:
    return json.dumps(data_id, sort_keys=True)


def deserialize_data_id(data_id_str) -> Any:
    return json.loads(data_id_str)


def now_timestamp() -> int:
    return int(datetime.now().timestamp() * 1000)


def deserialize_request_id(request_id_str):
    # request_id_str = request_id_key[len(self.crawler_name) + 1:]
    if integer_request_id_regex.match(request_id_str) is not None:
        return int(request_id_str)
    elif date_regex.match(request_id_str) is not None:
        year, month, date = request_id_str.split(" ")[0].split("-")
        return datetime(int(year), int(month), int(date))
    else:
        return request_id_str


def serialize_request_id(request_id) -> str:
    if isinstance(request_id, int):
        ret = "%010d" % request_id
    elif isinstance(request_id, datetime):
        assert request_id.hour == request_id.minute == request_id.second == request_id.microsecond == 0
        ret = "%s" % request_id.date()
    elif isinstance(request_id, str):
        assert integer_request_id_regex.match(request_id) is None and date_regex.match(request_id) is None, \
            "Type ambiguous request string " + request_id
        ret = "%s" % request_id
    else:
        assert False, "Unexpected request id type" + str(request_id)
    assert deserialize_request_id(ret) == request_id
    return ret


def s3_list_by_prefix(bucket_name, prefix) -> Iterable[str]:
    s3 = boto3.client('s3')
    ret = s3.list_objects(
        Bucket=bucket_name,
        Delimiter="/",
        Prefix=prefix,
        RequestPayer='requester'
    )
    for r in ret["Contents"]:
        yield r["Key"]
    while 'NextMarker' in ret:
        assert ret["IsTruncated"]
        ret = s3.list_objects(
            Bucket=bucket_name,
            Delimiter="/",
            Prefix=prefix,
            RequestPayer='requester',
            Marker=ret['NextMarker']
        )
        for r in ret["Contents"]:
            yield r["Key"]
    assert not ret["IsTruncated"]


def mongo_list_by_prefix(coll, prefix) -> Iterable[str]:
    for record in coll.find({"_id": {'$regex': '^' + re.escape(prefix)}}, {"_id": 1}):
        yield record["_id"]

_map_fn = None
_db = None
#_output_queue = Queue(10000)
def worker(req):
    request_id, data_id, version = _db._parse_data_key(req)
    body = _db.get_data(request_id, data_id, version)["Body"]
    my_ret = {"request_id": request_id,
              "data_id": data_id,
              "version": version,
              "data": body.read()}

    if _map_fn is not None:
        my_ret = _map_fn(my_ret)
    return my_ret

CRAWLED = 0
REQUESTED = 1
ERROR = 2


class CrawlDB:
    # s3 stuff
    s3 = boto3.client('s3')
    S3_BUCKET_NAME = os.environ.get("CRAWLER_S3_BUCKET", "askmiso-crawler")

    # suppress logging
    logging.getLogger('botocore').setLevel(logging.WARN)
    logging.getLogger('boto3').setLevel(logging.WARN)

    def __init__(self, crawler_name: str, request_timeout: timedelta = timedelta(minutes=10),
                 mongo_db=None):
        self.__version__ = "0.7.4"
        if mongo_db is None:
            self.status_coll = MongoClient("mongo").crawldb.status
            self.s3_key_cache = MongoClient("mongo").crawldb.s3_key_cache
        else:
            self.status_coll = mongo_db.crawldb.status
            self.s3_key_cache = mongo_db.crawldb.s3_key_cache

        # make sure the required index is available
        self.status_coll.create_index([("crawler", pymongo.ASCENDING),
                                       ("status", pymongo.ASCENDING),
                                       ("requested_time", pymongo.ASCENDING)])
        self.crawler_name = crawler_name

        assert isinstance(request_timeout, timedelta)
        self.request_timeout = request_timeout

    def _timeout_threshold(self) -> int:
        """
        :return: the requested_time threshold. Any uncommitted request with requested_time before this time is
        considered to be timeout.
        """
        return int((datetime.now() - self.request_timeout).timestamp() * 1000)

    def get_request_id_key(self, request_id) -> str:
        """
        :param request_id:
        :return: a string representation of crawler_name | request_id
        """
        return "|".join([self.crawler_name, serialize_request_id(request_id)])

    def _parse_request_id_key(self, request_id_key):
        request_id_str = request_id_key[len(self.crawler_name) + 1:]
        return deserialize_request_id(request_id_str)

    def _get_data_key(self, request_id, data_id, version: int):
        assert isinstance(version, int)
        return "/".join(map(urllib.parse.quote_plus,
                            [self.crawler_name,
                             serialize_request_id(request_id),
                             serialize_data_id(data_id),
                             str(version)
                             ]))

    def _parse_data_key(self, key: str) -> Tuple[Any, Any, int]:
        crawler_name, request_id_str, data_id_str, version_str = map(urllib.parse.unquote_plus, key.split("/"))
        assert crawler_name == self.crawler_name
        request_id = deserialize_request_id(request_id_str)
        data_id = deserialize_data_id(data_id_str)
        version = int(version_str)
        assert key == self._get_data_key(request_id, data_id, version), "%s != %s" % (key, self._get_data_key(request_id, data_id, version))
        return request_id, data_id, version

    def _get_data_key_prefix(self, request_id):

        return "/".join(map(urllib.parse.quote_plus,
                            [self.crawler_name,
                             serialize_request_id(request_id)
                             ]))

    def is_crawled_or_being_crawled(self, request_id):
        """
        Check if a request is being crawled or was already crawled
        :param request_id:
        :return: True or False
        """
        return self.status_coll.find({"_id": self.get_request_id_key(request_id)}).count() > 0

    def park_request(self, request_id):
        """
        Park a request so that other crawlers won't crawl it before it is timeout
        :param request_id: request id
        :return: None
        :raises: ConcurrentParkingException if other crawlers tried to park the request at the same time
        """
        status_record = self.status_coll.find_one(self.get_request_id_key(request_id))
        if status_record is None or (status_record["status"] == REQUESTED
                                     and status_record["requested_time"] < self._timeout_threshold()):
            self.status_coll.replace_one({"_id": self.get_request_id_key(request_id)},
                                         {"crawler": self.crawler_name,
                                          "request_id": serialize_request_id(request_id),
                                          "requested_time": now_timestamp(),
                                          "status": REQUESTED},
                                         True
                                         )
        else:
            raise ConcurrentParkingException

    def commit_request(self, request_id, meta=None, skip_park=False):
        """
        Commit a request. After a request is committed, it is considered finished.
        :param request_id: request id
        :param meta: metadata to attach the committed request
        :param skip_park: whether to allow the commit without parking the request first
        :return: None
        """
        status_record = self.status_coll.find_one(self.get_request_id_key(request_id))
        if (status_record is not None and status_record["status"] == REQUESTED) or skip_park:
            self.status_coll.replace_one({"_id": self.get_request_id_key(request_id)},
                                         {"crawler": self.crawler_name,
                                          "request_id": serialize_request_id(request_id),
                                          "committed_time": now_timestamp(),
                                          "metadata": meta,
                                          "status": REQUESTED},
                                         True
                                         )
        else:
            raise CommitBeforeParkingException()

    def get_request_meta(self, request_id) -> Optional[Any]:
        """
        :param request_id: request id
        :return: the metadata attached to the request when it was committed
        """
        record = self.status_coll.find_one(self.get_request_id_key(request_id))
        if record is not None:
            return record.get("metadata")


    def get_data_ids(self, request_id):
        """
        :param request_id: request id
        :return: Return the set of data ids that were uploaded for this request
        """
        prefix = self._get_data_key_prefix(request_id)
        data_ids = set()
        for key in mongo_list_by_prefix(self.s3_key_cache, prefix):
            request_id, data_id, version = self._parse_data_key(key)
            assert request_id == request_id
            data_ids.add(data_id)
        return data_ids

    def get_timeout_request_id(self) -> Optional[Any]:
        """
        :return: a request (if any) which was unable to be finished before timeout
        """
        record = self.status_coll.find_one({"status": REQUESTED,
                                            "crawler": self.crawler_name,
                                            "requested_time": {"$lt": self._timeout_threshold()}})
        if record is not None:
            return deserialize_request_id(record["request_id"])
        else:
            return None

    def save_data(self, request_id, data_id: Any, data, version: int = 0):
        """

        :param request_id: request id (number, datetime, or string)
        :param data_id: data id, anything that can be json seerialized
        :param data: bytes or file like object
        :param version: int (default 0)
        :return:
        """
        file_key = self._get_data_key(request_id, data_id, version)
        self.s3.put_object(Bucket=self.S3_BUCKET_NAME, Body=data, Key=file_key)
        self.s3_key_cache.replace_one({"_id": file_key}, {}, True)

    def get_data(self, request_id, data_id, version=0):
        if self.data_exists(request_id, data_id, version):
            file_key = self._get_data_key(request_id, data_id, version)
            return self.s3.get_object(Bucket=self.S3_BUCKET_NAME, Key=file_key)
        else:
            raise ValueError("request: %s, data id %s does not exist" % (request_id, data_id))

    def data_exists(self, request_id, data_id, version=0):
        return self.s3_key_cache.find_one(self._get_data_key(request_id, data_id, version)) is not None

    def list_data(self) -> Iterable[Tuple]:
        """
        :return: A iterable of tuples of (request_id, data_id, version)
        """
        return map(self._parse_data_key, mongo_list_by_prefix(self.s3_key_cache, self.crawler_name + "/"))

    def parallel_scan_items(self, thread_count=None, map_fn=None, executor_type="thread", reverse=True) -> Iterable[Any]:
        global _map_fn, _db
        _map_fn = map_fn
        _db = self

        if thread_count is None:
            thread_count = multiprocessing.cpu_count()

        requests = list(mongo_list_by_prefix(self.s3_key_cache, self.crawler_name + "/"))
        if reverse:
            requests = list(reversed(requests))

        with Parallel(n_jobs=thread_count) as parallel:
            for ret in parallel(delayed(worker)(i) for i in requests):
                yield ret



    def delete_request(self, request_id):
        self.status_coll.remove({"_id": self.get_request_id_key(request_id)})


class SequentialCrawlDB(CrawlDB):
    def __init__(self, crawler_name, request_timeout, initial_request_id, max_request_id, mongo_db=None):
        super(SequentialCrawlDB, self).__init__(crawler_name, request_timeout, mongo_db)

        assert isinstance(initial_request_id, int)
        assert isinstance(max_request_id, int)

        self.initial_request_id = initial_request_id
        self.max_request_id = max_request_id

    def _divide_and_conquer_search(self, start_request_id=None, end_request_id=None):
        """

        :param start_request_id: the current id
        :param end_request_id: the max id
        :return: a request id that is not been crawled between the start_request_id (inclusive) and end_request_id (inclusive)
        """
        if start_request_id is None:
            start_request_id = self.initial_request_id
        if end_request_id is None:
            end_request_id = self.max_request_id

        for i in range(start_request_id, min(end_request_id + 1, start_request_id+10)):
            if not self.is_crawled_or_being_crawled(i):
                return i
            if i == end_request_id:
                return None

        n_requests = self.status_coll.find({"_id": {"$gte": self.get_request_id_key(start_request_id),
                                                    "$lte": self.get_request_id_key(end_request_id)}}).count()
        if n_requests == start_request_id - end_request_id + 1:
            # no more gap!
            return None

        # do divide and counter
        middle = int(start_request_id + ((end_request_id - start_request_id) / 2))
        ret = self._divide_and_conquer_search(middle, end_request_id)
        if ret is not None:
            return ret
        else:
            ret = self._divide_and_conquer_search(start_request_id, middle)
            return ret

    def next_request_id(self):
        if self.initial_request_id is not None:
            ret = self._divide_and_conquer_search()
            self.initial_request_id = ret
            return ret
        else:
            return None


class SequentialTimeCrawlDB(CrawlDB):
    def __init__(self, crawler_name, request_timeout, initial_date: datetime,
                 time_interval: timedelta = timedelta(days=1), mongo_db=None):
        self.epoch = datetime(1900, 1, 1)
        self.initial_date = int((initial_date - self.epoch) / time_interval) * time_interval + self.epoch
        self.time_interval = time_interval
        super(SequentialTimeCrawlDB, self).__init__(crawler_name, request_timeout, mongo_db)

    def next_request(self, cur_request_date=None) -> datetime:
        if cur_request_date is None:
            cur_request_date = self.initial_date
        while self.is_crawled_or_being_crawled(cur_request_date):
            cur_request_date += self.time_interval
            if cur_request_date >= datetime.now():
                break
        return cur_request_date


class StringIDCrawlDB(CrawlDB):
    def __init__(self, crawler_name, request_timeout, mongo_db=None):
        super(StringIDCrawlDB, self).__init__(crawler_name, request_timeout, mongo_db)

