import unittest
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta, datetime
from moto import mock_s3
import boto3
import mongomock
from crawldb import SequentialCrawlDB, CommitBeforeParkingException, SequentialTimeCrawlDB


class Test(unittest.TestCase):
    def test_next_sequential(self):
        db = mongomock.MongoClient().db
        crawldb = SequentialCrawlDB("test", timedelta(seconds=10), 9, 10, mongo_db=db)
        # should return initial id
        self.assertEqual(crawldb.next_request_id(), 9)
        self.assertEqual(crawldb.next_request_id(), 9)

        # after we park a request, it should return next id
        crawldb.park_request(crawldb.next_request_id())
        self.assertEqual(crawldb.next_request_id(), 10)

        # after we park another request, it should return None

        crawldb.park_request(crawldb.next_request_id())
        self.assertEqual(crawldb.next_request_id(), None)
        self.assertEqual(crawldb.next_request_id(), None)

    def test_next_sequential_large_scale(self):
        db = mongomock.MongoClient().db
        max_id = 100
        crawldb = SequentialCrawlDB("test", timedelta(seconds=10), 0, max_id, mongo_db=db)
        for i in range(max_id):
            next_id = crawldb.next_request_id()
            self.assertEqual(next_id, i)
            crawldb.park_request(next_id)

        new_crawldb = SequentialCrawlDB("test", timedelta(seconds=10), 0, max_id, mongo_db=db)
        # should return initial id
        self.assertEqual(new_crawldb.next_request_id(), max_id)

    def test_commit_without_parking(self):
        db = mongomock.MongoClient().db
        crawldb = SequentialCrawlDB("test", timedelta(seconds=10), 0, 20, mongo_db=db)
        # should raise
        with self.assertRaises(CommitBeforeParkingException):
            crawldb.commit_request(request_id=10)

        crawldb.park_request(request_id=10)
        crawldb.commit_request(request_id=10)

        crawldb.commit_request(request_id=11, skip_park=True)

    @mock_s3
    def test_save_data(self):
        db = mongomock.MongoClient().db
        boto3.client('s3').create_bucket(Bucket=SequentialCrawlDB.S3_BUCKET_NAME)
        crawldb = SequentialCrawlDB("test", timedelta(seconds=10), 0, 20, mongo_db=db)
        data_set = {b"test is a test 1", b"test is a test 2", b"test is a test 3"}
        for d, index in zip(data_set, range(len(data_set))):
            crawldb.save_data(1, index, d)


        #self.assertEqual(set([item["data"] for item in crawldb.parallel_scan_items()]), data_set)
        #self.assertEqual(crawldb.get_data_ids(1), {0, 1, 2})

        # check parsing/serializing data id
        data_id_with_symbol = "2011-02-17-0+00 00.json.gz/32r422//&&@#$@@!~"
        crawldb.save_data(datetime(2012, 1, 2), data_id_with_symbol, b"test")
        self.assertEqual(set(crawldb.get_data_ids(datetime(2012, 1, 2))),  {data_id_with_symbol})

    @mock_s3
    def test_delete_data(self):
        db = mongomock.MongoClient().db
        boto3.client('s3').create_bucket(Bucket=SequentialCrawlDB.S3_BUCKET_NAME)
        crawldb = SequentialCrawlDB("test", timedelta(seconds=10), 0, 20, mongo_db=db)
        data_set = [b"test is a test 1", b"test is a test 2", b"test is a test 3"]
        for d, index in zip(data_set, range(len(data_set))):
            crawldb.save_data(1, index, d)
        crawldb.commit_request(1, skip_park=True)
        crawldb.delete_data(1, 0)
        self.assertEquals(set(crawldb.get_data_ids(1)), {1, 2})
        self.assertEquals(crawldb.get_timeout_request_id(), 1)

    def test_metadata(self):
        db = mongomock.MongoClient().db
        crawldb = SequentialCrawlDB("test", timedelta(seconds=10), 0, 20, mongo_db=db)
        # should raise
        crawldb.commit_request(1, {"test": "test"}, skip_park=True)
        self.assertEqual(crawldb.get_request_meta(1), {"test": "test"})
        # return none if not requested
        self.assertEqual(crawldb.get_request_meta(10), None)

    def test_timeout(self):
        db = mongomock.MongoClient().db
        crawldb = SequentialCrawlDB("test", timedelta(milliseconds=10), 0, 20, mongo_db=db)
        crawldb.park_request(1)
        import time
        time.sleep(0.1)
        self.assertEqual(1, crawldb.get_timeout_request_id())

        db = mongomock.MongoClient().db
        crawldb = SequentialCrawlDB("test", timedelta(milliseconds=2000), 0, 20, mongo_db=db)
        crawldb.park_request(1)
        import time
        time.sleep(0.1)
        self.assertEqual(None, crawldb.get_timeout_request_id())


    def test_datetime(self):
        db = mongomock.MongoClient().db
        crawldb = SequentialTimeCrawlDB("test", timedelta(milliseconds=10), initial_date=datetime(2000,1,1), mongo_db=db)
        self.assertEqual(crawldb.next_request(), datetime(2000, 1, 1))
        crawldb.park_request(crawldb.next_request())
        self.assertEqual(crawldb.next_request(), datetime(2000, 1, 2))
