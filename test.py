#!/usr/bin/env python

import logging
# import time

from kinesis_producer import KinesisProducer

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('botocore').setLevel(logging.WARNING)

log = logging.getLogger(__name__)

config = dict(
    buffer_size_limit=100000,
    buffer_time_limit=0.2,
    record_delimiter='\n',
    stream_name='jz-python-devlocal',
    kinesis_max_retries=3,
    aws_region='us-east-1',
    )

k = KinesisProducer(config=config)

payload = '{MSG:%%05i %s}' % ('BlaBl' * 198)

try:
    print ' <> MSGS'
    for msg_id in range(1500):
        record = payload % msg_id
        k.send(record)
        # time.sleep(0.0001)

except KeyboardInterrupt:
    pass
finally:
    print ' <> STOP'
    k.stop()

    print ' <> JOIN'
    k.join()

    print ' <> QUIT'
