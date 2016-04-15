#!/usr/bin/env python

import logging
import time

from kinesis_producer import KinesisProducer

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('botocore').setLevel(logging.WARNING)

log = logging.getLogger(__name__)

config = dict(
    aws_region='us-east-1',
    buffer_size_limit=200000,
    buffer_time_limit=0.2,
    kinesis_concurrency=4,
    kinesis_max_retries=10,
    record_delimiter='\n',
    stream_name='jz-python-devlocal',
    )

k = KinesisProducer(config=config)

payload = '{MSG:%%05i %s}' % ('X' * 1000)

try:
    print ' <> MSGS'
    for msg_id in range(50000):
        record = payload % msg_id
        k.send(record)
        # time.sleep(0.2)

    # time.sleep(5)
except KeyboardInterrupt:
    pass
finally:
    print ' <> CLOSE'
    k.close()

    print ' <> JOIN'
    k.join()

    print ' <> QUIT'
