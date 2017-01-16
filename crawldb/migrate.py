from decimal import Decimal
import tqdm

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
crawl_data_db = dynamodb.Table('crawl_data')

def preprocess_item(item):
    # merge multi-part data

    # convert to mutable bytearray
    data_buf = bytearray(item["data"].value)  # type: bytearray
    # process data with multiple parts
    if "part_id" in item:
        # get all the rest parts
        for part in range(1, int(item["total_parts"])):
            res = crawl_data_db.get_item(
                Key={"crawler_name_and_request_id": item["crawler_name_and_request_id"] + "|ext",
                     "data_id": item["data_id"] + "|" + str(part)})
            if "Item" in res:
                data_buf.extend(res["Item"]["data"].value)
            else:
                raise MissingDataPart(item["crawler_name_and_request_id"] + "," + item["data_id"])
    # convert back to bytes (immutable)
    item["data"] = bytes(data_buf)
    assert len(item["crawler_name_and_request_id"].split("|")) == 2
    item["request_id"] = deserialize_request_id(item["crawler_name_and_request_id"].split("|")[1])
    item["crawler_name"] = item["crawler_name_and_request_id"].split("|")[0]
    item["data_id"] = deserialize_data_id(item["data_id"])
    if "version" in item:
        version_decimal = item["version"]  # type: Decimal
        item["version"] = int(version_decimal)
    else:
        item["version"] = 0
    return item


def scan_items(last_item=None, segment=0, total_segments=1):
    import time
    while True:
        try:
            if last_item is None:
                res = crawl_data_db.scan(Select="ALL_ATTRIBUTES",
                                         Segment=segment,
                                         TotalSegments=total_segments)
            else:
                res = crawl_data_db.scan(
                    Select="ALL_ATTRIBUTES",
                    ExclusiveStartKey=last_item['key'],
                    Segment=segment,
                    TotalSegments=total_segments
                )
            break
        except ClientError as e:
            if e.response['Error']['Code'] == "ProvisionedThroughputExceededException":
                time.sleep(2)
                continue
            else:
                raise e
    for i in res["Items"]:
        if not i["crawler_name_and_request_id"].endswith("|ext"):
            try:
                yield preprocess_item(i)
            except Exception as e:
                logging.error(e, "Failed to preprocess item " + str(i))

    backoff_time = 1
    while 'LastEvaluatedKey' in res:
        try:

            res = crawl_data_db.scan(
                Select="ALL_ATTRIBUTES",
                ExclusiveStartKey=res['LastEvaluatedKey'],
                Segment=segment,
                TotalSegments=total_segments
            )
            for i in res["Items"]:
                if not i["crawler_name_and_request_id"].endswith("|ext"):
                    try:
                        yield preprocess_item(i)
                    except Exception as e:
                        logging.error(e, "Failed to preprocess item " + str(i))

        except ClientError as e:
            if e.response['Error']['Code'] == "ProvisionedThroughputExceededException" and backoff_time <= 10240:
                # exponential backoff when error
                time.sleep(backoff_time)
                backoff_time *= 2
                continue
            else:
                raise e


def parallel_scan_items(thread_count, map_fn=None, queue_size=10000):
    import threading
    from queue import Queue
    worker_end = object()

    def worker(thread_id, thread_count, queue, int_event, map_fn):
        try:
            for item in scan_items(segment=thread_id, total_segments=thread_count):
                try:
                    if map_fn is None:
                        queue.put(item)
                    else:
                        queue.put(map_fn(item))
                    if int_event.is_set():
                        logging.log(logging.WARN, "Thread %d Interrupted" % thread_id)
                        break
                except Exception as e:
                    logging.error(e)
                    queue.put(e)
        finally:
            queue.put(worker_end)

    queue = Queue(queue_size)
    interruptevent = threading.Event()
    threads = []
    try:
        for i in range(thread_count):
            t = threading.Thread(target=worker, args=(i, thread_count, queue, interruptevent, map_fn))
            # t.daemon = True
            t.start()
            threads.append(t)
        finish_count = 0
        while True:
            item = queue.get()
            if item is worker_end:
                finish_count += 1
                if finish_count == thread_count:
                    break
            else:
                yield item
    finally:
        logging.log(logging.WARN, "Interrupt threads")
        interruptevent.set()





def get_data_key(crawler_name, request_id, data_id, version):
    assert isinstance(version, int)
    return "/".join(map(urllib.parse.quote_plus,
                        [crawler_name,
                         serialize_request_id(request_id),
                         serialize_data_id(data_id),
                         str(version)
                         ]))


s3 = boto3.client('s3')
S3_BUCKET_NAME = "miso-crawler"


def upload(item):
    if item["crawler_name"] not in {"ieee"}:
        try:
            key = get_data_key(item["crawler_name"], item["request_id"], item["data_id"], item["version"])
            if key not in all_keys:
                s3.put_object(Bucket=S3_BUCKET_NAME, Body=item["data"], Key=key)
        except Exception as e:
            return item, e
    return None


failed = []
for ret in tqdm.tqdm(parallel_scan_items(16, map_fn=upload), total=40 * 1000000):
    if isinstance(ret, tuple):
        failed.append(ret)
        print(ret)

bar = tqdm.tqdm()
all_keys = set()
ret = s3.list_objects(
    Bucket=S3_BUCKET_NAME,

    Prefix="",
    RequestPayer='requester'
)
last_key = None
for r in ret["Contents"]:
    all_keys.add(r["Key"])
    bar.update()
    last_key = r["Key"]
while ret["IsTruncated"]:
    assert ret["IsTruncated"]
    ret = s3.list_objects(
        Bucket=S3_BUCKET_NAME,


        RequestPayer='requester',
        Marker=last_key
    )
    for r in ret["Contents"]:
        all_keys.add(r["Key"])
        bar.update()
        last_key = r["Key"]
assert not ret["IsTruncated"]