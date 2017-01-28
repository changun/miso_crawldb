import unittest
from datetime import timedelta

import boto3
import mongomock
from moto import mock_s3

from crawldb import SequentialCrawlDB


class Test(unittest.TestCase):
    @mock_s3
    def testMigrate(self):
        boto3.client('s3').create_bucket(Bucket=SequentialCrawlDB.S3_BUCKET_NAME)
        db = mongomock.MongoClient().db
        crawldb = SequentialCrawlDB("test", timedelta(seconds=10), 9, 10, mongo_db=db)
        # populate some data
        for i in range(100):
            crawldb.save_data(i, i, b"")
            crawldb.commit_request(i, skip_park=True)
        # remove the status coll
        crawldb.status_coll.drop()
        # those requests should be uncommitted
        for i in range(100):
            self.assertEquals(crawldb.is_crawled_or_being_crawled(i), False)
        # migrate requests based on data records
        for r_id, _, _ in crawldb.list_data():
            crawldb.commit_request(r_id, skip_park=True)
        for i in range(100):
            self.assertEquals(crawldb.is_crawled_or_being_crawled(i), True)
