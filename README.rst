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
       aws_region='us-east-1',
       buffer_size_limit=100000,
       buffer_time_limit=0.2,
       kinesis_concurrency=1,
       kinesis_max_retries=10,
       record_delimiter='\n',
       stream_name='KINESIS_STREAM_NAME',
       )

   k = KinesisProducer(config=config)

   for record in records:
       k.send(record)

   k.close()
   k.join()


Config
======

:aws_region: AWS region for Kinesis calls (like us-east-1)
:buffer_size_limit:
   Approximative size limit for record aggregation (in bytes)
:buffer_time_limit:
   Approximative time limit for record aggregation (in seconds)
:kinesis_concurrency:
   Set the concurrency level for Kinesis calls. Set to 1 for no
   concurrency. Set to 2 and more to use a thread pool.
:kinesis_max_retries:
   Number of Kinesis put_records call attempt before giving up.
   This number should be between 4 and 10 if you want to handle
   temporary ProvisionedThroughputExceeded errors.
:record_delimiter: Delimiter for record aggregation
:stream_name: Name of the Kinesis Stream


Kinesis retries
---------------

Kinesis calls are retried for ProvisionedThroughputExceeded error
only. Retry use an exponential backoff logic (0.1s, 0.2s, 0.4s, 0.8s,
1.60s, 3.20s, 6.40s, 12.80s, 25.60s, 51.20s, 102.40s...)


Copyright and license
=====================

Released under the MIT license.