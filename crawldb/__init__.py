import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import json
import math
from enum import Enum
import logging


class CrawlStatus(Enum):
    CRAWLED = 1
    REQUESTED = 0

class CommitBeforeParkingException(Exception):
    pass

class ConcurrentParkingException(Exception):
    pass

class MissingDataPart(Exception):
    pass

class CrawlDB:
    def __init__(self, crawler_name, request_timeout):
        self.crawler_name = crawler_name
        self.request_timeout = request_timeout
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        # suppress logging
        logging.getLogger('botocore').setLevel(logging.WARN)
        logging.getLogger('boto3').setLevel(logging.WARN)

        self.crawl_request_db = self.dynamodb.Table('crawl_request')
        self.crawl_data_db = self.dynamodb.Table('crawl_data')

    def now_timestamp(self):
        return int(datetime.now().timestamp() * 1000)

    def timeout_threshold(self):
        return int((datetime.now() - self.request_timeout).timestamp() * 1000)

    def get_request_id_key(self, request_id):
        if isinstance(request_id, int):
            return "|".join([str(self.crawler_name), "%010d" % request_id])

    def parse_request_id_key(self, request_id_key):
        return int(request_id_key[len(self.crawler_name) + 1:])

    def get_request_status_key(self, status):
        return "|".join([str(self.crawler_name), str(status.value)])

    def drop(self):
        items = self.crawl_request_db.query(
            IndexName="request_status",
            KeyConditionExpression=Key('crawler_name_and_status').eq(self.get_request_status_key(CrawlStatus.REQUESTED))
        )["Items"]
        items += self.crawl_request_db.query(
            IndexName="request_status",
            KeyConditionExpression=Key('crawler_name_and_status').eq(self.get_request_status_key(CrawlStatus.CRAWLED))
        )["Items"]
        print("Removing " + str(len(items)) + " items")
        for item in items:
            self.crawl_request_db.delete_item(Key={"crawler_name_and_request_id": item["crawler_name_and_request_id"]})

    def is_crawled_or_being_crawled(self, request_id):
        return self.crawl_request_db.query(
            Select='COUNT',
            KeyConditionExpression=Key('crawler_name_and_request_id').eq(self.get_request_id_key(request_id))
        )["Count"] > 0

    def park_request(self, request_id):
        try:
            return self.crawl_request_db.put_item(
                Item=
                {'crawler_name_and_request_id': self.get_request_id_key(request_id),
                 'crawler_name_and_status': self.get_request_status_key(CrawlStatus.REQUESTED),
                 'requested_time': self.now_timestamp(),
                 'crawler_name': self.crawler_name},
                ConditionExpression=
                "(attribute_not_exists(crawler_name_and_status) OR crawler_name_and_status = :status) " +
                "AND (attribute_not_exists(requested_time) OR  requested_time < :time)",
                ExpressionAttributeValues={
                    ':time': self.timeout_threshold(),
                    ':status': self.get_request_status_key(CrawlStatus.REQUESTED)
                }
            )
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                raise ConcurrentParkingException()
            else:
                raise e

    def commit_request(self, request_id, meta=None, skip_park=False, batch_writer=None):
        if meta is None:
            meta = {}
        if skip_park:
            # do not check if a requested is parked
            if batch_writer is None:
                # use the batch_writer if given one
                batch_writer = self.crawl_request_db
            return batch_writer.put_item(
                Item=
                {'crawler_name_and_request_id': self.get_request_id_key(request_id),
                 'crawler_name_and_status': self.get_request_status_key(CrawlStatus.CRAWLED),
                 'requested_time': self.now_timestamp(),
                 'meta': meta,
                 'crawler_name': self.crawler_name})
        else:
            try:
                # if the request was not parked, raise CommitBeforeParkingException
                return self.crawl_request_db.update_item(
                    Key={'crawler_name_and_request_id': self.get_request_id_key(request_id)},
                    UpdateExpression="set meta = :meta, crawler_name_and_status=:status",
                    ConditionExpression="attribute_exists(requested_time)",
                    ExpressionAttributeValues={
                        ':meta': meta,
                        ':status': self.get_request_status_key(CrawlStatus.CRAWLED)
                    })
            except ClientError as e:
                if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                    raise CommitBeforeParkingException()
                else:
                    raise e

    def get_request_meta(self, request_id):
        items = self.crawl_request_db.query(
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression="meta",
            KeyConditionExpression=Key('crawler_name_and_request_id').eq(self.get_request_id_key(request_id))
        )["Items"]
        if len(items) > 0:
            print(items[0])
            return items[0]["meta"]
        else:
            return None

    def get_data_ids(self, request_id):
        data_ids = []

        response = self.crawl_data_db.query(
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression="data_id",
            KeyConditionExpression=Key('crawler_name_and_request_id').eq(self.get_request_id_key(request_id)))
        data = response["Items"]
        while 'LastEvaluatedKey' in response:
            response = self.crawl_data_db.query(
                Select='SPECIFIC_ATTRIBUTES',
                ProjectionExpression="data_id",
                KeyConditionExpression=Key('crawler_name_and_request_id').eq(self.get_request_id_key(request_id)),
                ExclusiveStartKey=response['LastEvaluatedKey'])

            data.extend(response['Items'])
        for d in data:
            data_ids.append(json.loads(d["data_id"]))
        return data_ids

    def get_timeout_request_id(self):
        ret = self.crawl_request_db.query(
            Limit=1,
            IndexName="request_status",
            KeyConditionExpression=
            Key('crawler_name_and_status').eq(self.get_request_status_key(CrawlStatus.REQUESTED)) &
            Key('requested_time').lt(self.timeout_threshold())
        )["Items"]

        if len(ret) > 0:
            return self.parse_request_id_key(ret[0]['crawler_name_and_request_id'])
        else:
            return None

    def batch_data_write(self):
        return self.crawl_data_db.batch_writer()

    def save_data(self, request_id, data_id, data, batch_writer=None, version=0):
        data_id_str = json.dumps(data_id, sort_keys=True)
        max_part_size = 390 * 1024
        if batch_writer is None:
            batch_writer = self.crawl_data_db
        while True:
            backoff_time = 1
            try:
                if len(data) <= max_part_size:
                    return batch_writer.put_item(Item=
                                                 {'crawler_name_and_request_id': self.get_request_id_key(request_id),
                                                  'data_id': data_id_str,
                                                  'data': data,
                                                  'crawled_time': self.now_timestamp(),
                                                  'crawler_name': self.crawler_name,
                                                  'version': version})
                else:
                    total_parts = math.ceil(len(data) / max_part_size)
                    crawled_time = self.now_timestamp()
                    for part_id in range(total_parts):
                        cur_part = data[part_id * max_part_size: min((part_id + 1) * max_part_size, len(data))]
                        if part_id == 0:
                            batch_writer.put_item(Item=
                                                  {'crawler_name_and_request_id': self.get_request_id_key(request_id),
                                                   'data_id': data_id_str,
                                                   'data': cur_part,
                                                   'total_parts': total_parts,
                                                   'total_size': len(data),
                                                   'part_id': part_id,
                                                   'crawled_time': crawled_time,
                                                   'crawler_name': self.crawler_name,
                                                   'version': version})
                        else:
                            batch_writer.put_item(Item=
                                                  {'crawler_name_and_request_id': self.get_request_id_key(
                                                      request_id) + "|ext",
                                                   'data_id': data_id_str + ("|%d" % part_id),
                                                   'head_data_id': data_id_str,
                                                   'head_crawler_name_and_request_id': self.get_request_id_key(
                                                       request_id),
                                                   'data': cur_part,
                                                   'total_parts': total_parts,
                                                   'total_size': len(data),
                                                   'part_id': part_id,
                                                   'crawled_time': crawled_time,
                                                   'crawler_name': self.crawler_name})
                break
            except ClientError as e:
                if e.response['Error']['Code'] == "ProvisionedThroughputExceededException" and backoff_time <= 1024:
                    import time
                    # exponential backoff when error
                    time.sleep(backoff_time)
                    backoff_time *= 2
                else:
                    raise e

    def preprocess_item(self, item):
        # merge multi-part data
        # covert item["data"] from Binary to bytes

        data_buf = item["data"].value
        if "part_id" in item:
            for part in range(1, int(item["total_parts"])):
                res = self.crawl_data_db.get_item(
                    Key={"crawler_name_and_request_id": item["crawler_name_and_request_id"] + "|ext",
                         "data_id": item["data_id"] + "|" + str(part)})
                if "Item" in res:
                    data_buf += res["Item"]["data"].value
                else:
                    raise MissingDataPart(item["crawler_name_and_request_id"] + "," + item["data_id"])

        item["data"] = data_buf
        item["request_id"] = self.parse_request_id_key(item["crawler_name_and_request_id"])
        item["key"] = {"crawler_name_and_request_id": item["crawler_name_and_request_id"], "data_id": item["data_id"]}
        return item

    def get_data(self, request_id, data_id):
        response = self.crawl_data_db.get_item(
            Key={"crawler_name_and_request_id": self.get_request_id_key(request_id),
                 "data_id": json.dumps(data_id, sort_keys=True)})
        if "Item" in response:
            return self.preprocess_item(response["Item"])

    def data_exists(self, request_id, data_id):
        response = self.crawl_data_db.get_item(
            ProjectionExpression="data_id",
            Key={"crawler_name_and_request_id": self.get_request_id_key(request_id),
                 "data_id": json.dumps(data_id, sort_keys=True)})
        return "Item" in response

    def scan_items(self, last_item=None, segment=0, total_segments=1):
        if last_item is None:
            res = self.crawl_data_db.scan(Select="ALL_ATTRIBUTES",
                                          Segment=segment,
                                          TotalSegments=total_segments,
                                          FilterExpression=Key("crawler_name").eq(self.crawler_name))
        else:
            res = self.crawl_data_db.scan(
                Select="ALL_ATTRIBUTES",
                FilterExpression=Key("crawler_name").eq(self.crawler_name),
                ExclusiveStartKey=last_item['key'],
                Segment=segment,
                TotalSegments=total_segments
            )

        for i in res["Items"]:
            yield self.preprocess_item(i)
        backoff_time = 1
        while 'LastEvaluatedKey' in res:
            try:

                res = self.crawl_data_db.scan(
                    Select="ALL_ATTRIBUTES",
                    FilterExpression=Key("crawler_name").eq(self.crawler_name),
                    ExclusiveStartKey=res['LastEvaluatedKey'],
                    Segment=segment,
                    TotalSegments=total_segments
                )
                for i in res["Items"]:
                    try:
                        yield self.preprocess_item(i)
                    except MissingDataPart:
                        pass
            except ClientError as e:
                if e.response['Error']['Code'] == "ProvisionedThroughputExceededException" and backoff_time <= 1024:
                    import time
                    # exponential backoff when error
                    time.sleep(backoff_time)
                    backoff_time *= 2
                    continue
                else:
                    raise e

    def delete_request(self, request_id):
        # delete the request entry
        self.crawl_request_db.delete_item(Key={"crawler_name_and_request_id": self.get_request_id_key(request_id)})
        # now, delete the data entries
        items = self.crawl_data_db.query(
            KeyConditionExpression=Key('crawler_name_and_request_id').eq(self.get_request_id_key(request_id))
        )["Items"]
        for i in items:
            print("Remove " + i["data_id"])
            self.crawl_data_db.delete_item(Key={"crawler_name_and_request_id": self.get_request_id_key(request_id),
                                                "data_id": i["data_id"]})

    def count_data(self, request_id):
        return self.crawl_data_db.query(
            Select='COUNT',
            KeyConditionExpression=Key('crawler_name_and_request_id').eq(self.get_request_id_key(request_id))
        )["Count"]


class SequentialCrawlDB(CrawlDB):
    def __init__(self, crawler_name, request_timeout, initial_request_id, max_request_id):
        super(SequentialCrawlDB, self).__init__(crawler_name, request_timeout)
        self.initial_request_id = initial_request_id
        self.max_request_id = max_request_id

    def count_range(self, start, end):

        res = self.crawl_request_db.query(
            IndexName='crawler_name_request_id',
            Select='COUNT',
            KeyConditionExpression=Key('crawler_name').eq(self.crawler_name) & \
                                   Key('crawler_name_and_request_id').between(
                                       self.get_request_id_key(start), self.get_request_id_key(end))
        )
        count = res["Count"]
        while 'LastEvaluatedKey' in res:
            res = self.crawl_request_db.query(
                IndexName='crawler_name_request_id',
                Select='COUNT',
                KeyConditionExpression=Key('crawler_name').eq(self.crawler_name) & \
                                       Key('crawler_name_and_request_id').between(
                                           self.get_request_id_key(start), self.get_request_id_key(end)),
                ExclusiveStartKey=res['LastEvaluatedKey']
            )
            count += res["Count"]
        return count

    def numer_of_possibe_requests(self, start, end):
        return end - start + 1

    def middle_request_id(self, start, end):
        return int((start + end) / 2)

    def inc_request_id(self, rid):
        return rid + 1

    def dec_request_id(self, rid):
        return rid - 1

    def random_search(self):
        import random
        for _ in range(10):
            ret = random.randint(self.initial_request_id, self.max_request_id)
            if not self.is_crawled_or_being_crawled(ret):
                return ret

    def sequential_search(self, cur_request_id=None, max_request_id=None):
        if cur_request_id is None:
            cur_request_id = self.initial_request_id
        if max_request_id is None:
            max_request_id = self.max_request_id
        print(cur_request_id, max_request_id)

        if max_request_id - cur_request_id < 10:
            for i in range(cur_request_id, max_request_id + 1):
                if not self.is_crawled_or_being_crawled(i):
                    return i
            return None
        elif max_request_id - cur_request_id < 1000:
            n_requests = self.count_range(cur_request_id, max_request_id)
            if n_requests >= self.numer_of_possibe_requests(cur_request_id, max_request_id):
                # no more gap!
                return None
                # there is some gap between cur_request_id and max_request_id

        # do divide and counter
        middle = int(cur_request_id + ((max_request_id - cur_request_id) / 2))
        ret = self.sequential_search(cur_request_id, middle)
        if ret is not None:
            return ret
        else:
            ret = self.sequential_search(middle, max_request_id)
            return ret

    def next_request_id(self, cur_request_id=None, max_request_id=None):
        ret = self.random_search()
        if ret is not None:
            return ret
        ret = self.sequential_search(cur_request_id, max_request_id)
        self.initial_request_id = ret
        return ret


class SequentialTimeCrawlDB(CrawlDB):
    def __init__(self, crawler_name, request_timeout, initial_date, time_interval=timedelta(days=1)):
        self.epoch = datetime(1900, 1, 1)
        self.initial_date = int((initial_date - self.epoch) / time_interval) * time_interval + self.epoch
        self.time_interval = time_interval
        super(SequentialTimeCrawlDB, self).__init__(crawler_name, request_timeout)

    def next_request(self, cur_request_date=None):
        if cur_request_date is None:
            cur_request_date = self.initial_date
        while self.is_crawled_or_being_crawled(cur_request_date):
            cur_request_date += self.time_interval
            if cur_request_date >= datetime.now():
                break
        return cur_request_date

    def get_request_id_key(self, request_id):
        return "|".join([str(self.crawler_name), str(request_id.date())])

    def parse_request_id_key(self, request_id_key):
        year, month, date = request_id_key[len(self.crawler_name) + 1:].split("-")
        return datetime(int(year), int(month), int(date))


class StringIDCrawlDB(CrawlDB):
    def __init__(self, crawler_name, request_timeout):
        super(StringIDCrawlDB, self).__init__(crawler_name, request_timeout)

    def get_request_id_key(self, request_id):
        return "|".join([str(self.crawler_name), request_id])

    def parse_request_id_key(self, request_id_key):
        return request_id_key[len(self.crawler_name) + 1:]

#
# def create_crawl_request_db():
#     crawl_request_db = dynamodb.create_table(
#         TableName='crawl_request',
#         KeySchema=[
#             {
#                 'AttributeName': 'crawler_name_and_request_id',
#                 'KeyType': 'HASH'  # Partition key
#             }
#         ],
#         AttributeDefinitions=[
#             {
#                 'AttributeName': 'crawler_name_and_request_id',
#                 'AttributeType': 'S'
#             },
#             {
#                 'AttributeName': 'crawler_name_and_status',
#                 'AttributeType': 'S'
#             },
#             {
#                 'AttributeName': 'requested_time',
#                 'AttributeType': 'N'
#             },
#             {
#                 'AttributeName': 'crawler_name',
#                 'AttributeType': 'S'
#             }
#         ],
#         GlobalSecondaryIndexes=[
#             {
#                 'IndexName': 'request_status',
#                 'KeySchema': [
#                     {
#                         'AttributeName': 'crawler_name_and_status',
#                         'KeyType': 'HASH'
#                     },
#                     {
#                         'AttributeName': 'requested_time',
#                         'KeyType': 'RANGE'
#                     },
#                 ],
#                 'Projection': {
#                     'ProjectionType': 'KEYS_ONLY'
#                 },
#                 'ProvisionedThroughput': {
#                     'ReadCapacityUnits': 1,
#                     'WriteCapacityUnits': 1
#                 }
#             }, {
#                 'IndexName': 'crawler_name_request_id',
#                 'KeySchema': [
#                     {
#                         'AttributeName': 'crawler_name',
#                         'KeyType': 'HASH'
#                     },
#                     {
#                         'AttributeName': 'crawler_name_and_request_id',
#                         'KeyType': 'RANGE'
#                     },
#                 ],
#                 'Projection': {
#                     'ProjectionType': 'KEYS_ONLY'
#                 },
#                 'ProvisionedThroughput': {
#                     'ReadCapacityUnits': 1,
#                     'WriteCapacityUnits': 1
#                 }
#             },
#         ],
#         ProvisionedThroughput={
#             'ReadCapacityUnits': 50,
#             'WriteCapacityUnits': 50
#         }
#     )
#
#     print("Table status:", crawl_request_db.table_status)
#     return crawl_request_db
# def create_crawl_data_db():
#     crawl_data_db = dynamodb.create_table(
#         TableName='crawl_data',
#         KeySchema=[
#             {
#                 'AttributeName': 'crawler_name_and_request_id',
#                 'KeyType': 'HASH'  # Partition key
#             },
#             {
#                 'AttributeName': 'data_id',
#                 'KeyType': 'RANGE'  # Partition key
#             }
#         ],
#         AttributeDefinitions=[
#             {
#                 'AttributeName': 'crawler_name_and_request_id',
#                 'AttributeType': 'S'
#             },
#             {
#                 'AttributeName': 'data_id',
#                 'AttributeType': 'S'
#             },
#             {
#                 'AttributeName': 'crawler_name',
#                 'AttributeType': 'S'
#             },
#             {
#                 'AttributeName': 'crawled_time',
#                 'AttributeType': 'N'
#             }
#         ],
#         ProvisionedThroughput={
#             'ReadCapacityUnits': 1,
#             'WriteCapacityUnits': 200
#         }
#     )
#     print("Table status:", crawl_data_db.table_status)
#     return crawl_data_db
# def test():
#     from datetime import timedelta
#     import time
#     db = SequentialCrawlDB(crawler_name="test", request_timeout=timedelta(seconds=1), initial_request_id=0,
#                            max_request_id=100)
#     db.drop()
#     rid = db.next_request()
#     assert rid == 0
#     db.park_request(rid)
#     assert db.next_request() == 1, db.next_request()
#     db.commit_request(rid)
#     db.park_request(db.next_request())
#     time.sleep(2)
#     assert db.get_timeout_request_id() == 1
#
#     # test commit before parking
#     capture = None
#     try:
#         db.commit_request(2)
#     except CommitBeforeParkingException as e:
#         capture = e
#     assert capture is not None
#     db.commit_request(2, skip_park=True)
#     db.drop()
#     # test race condition for parking
#     db.park_request(0)
#     capture = None
#     try:
#         # should fail, because someone already park the request
#         db.park_request(0)
#     except ClientError as e:
#         capture = e
#     assert capture is not None
#     time.sleep(1)
#     # should succeed becuase it is timeout
#     db.park_request(0)
#
#     db = SequentialTimeCrawlDB(crawler_name="test", request_timeout=timedelta(seconds=1),
#                                initial_date=datetime(2000, 1, 1), time_interval=timedelta(days=1))
#     db.drop()
#     rid = db.next_request()
#     assert rid == datetime(2000, 1, 1)
#     db.park_request(rid)
#     assert db.next_request() == datetime(2000, 1, 2), db.next_request()
#     db.commit_request(rid)
#     db.drop()
#     db = SequentialTimeCrawlDB(crawler_name="test", request_timeout=timedelta(seconds=1), initial_date=datetime.now(),
#                                time_interval=timedelta(days=1))
#     rid = db.next_request()
#     db.park_request(rid)
#     rid = db.next_request()
#     db.park_request(rid)
#     assert rid == db.next_request(), db.next_request()
#     db.drop()
