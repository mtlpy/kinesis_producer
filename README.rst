================
Kinesis Producer
================

Python producer for AWS Kinesis Stream with record aggregation.

It uses Boto 3 and is tested on Python 2.7 and 3.4/3.5.

* PyPI: https://pypi.python.org/pypi/kinesis_producer
* Code: https://github.com/ludia/kinesis_producer
* |travis|
* |coveralls|

.. |travis| image:: https://travis-ci.org/ludia/kinesis_producer.svg?branch=master
   :target: https://travis-ci.org/ludia/kinesis_producer

.. |coveralls| image:: https://coveralls.io/repos/github/ludia/kinesis_producer/badge.svg?branch=master
   :target: https://coveralls.io/github/ludia/kinesis_producer?branch=master


Install
=======

   pip install 'kinesis_producer < 1'

**Note**: Kinesis Producer use semver: you should always freeze on the major
version since it could mean breaking the API.


Usage
=====

Send records aggregated up to 100KB, 200ms and joined with '\\n':

.. code:: python

   from kinesis_producer import KinesisProducer

   config = dict(
       buffer_size_limit=100000,
       buffer_time_limit=0.2,
       record_delimiter='\n',
       stream_name='KINESIS_STREAM_NAME',
       kinesis_max_retries=3,
       aws_region='us-east-1',
       )

   k = KinesisProducer(config=config)

   for record in records:
       k.send(record)

   k.close()
   k.join()


Config
======

- ``stream_name``: Name of the Kinesis Stream
- ``kinesis_max_retries``: Number of Kinesis put_records call attempt before giving up
- ``aws_region``: AWS region for Kinesis calls
- ``buffer_size_limit``: Approximative size limit for record aggregation
- ``record_delimiter``: Delimiter for record aggregation
- ``buffer_time_limit``: Approximative time limit for record aggregation


Copyright and license
=====================

Released under the MIT license.