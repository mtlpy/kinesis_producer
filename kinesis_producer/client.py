import logging
import time
from multiprocessing.pool import ThreadPool

import boto3
import botocore

log = logging.getLogger(__name__)


def get_connection(aws_region):
    session = boto3.session.Session()
    connection = session.client('kinesis', region_name=aws_region)
    return connection


def call_and_retry(boto_function, max_retries, **kwargs):
    """Retry Logic for generic boto client calls.

    This code follows the exponetial backoff pattern suggested by
    http://docs.aws.amazon.com/general/latest/gr/api-retries.html
    """
    retries = 0
    while True:
        if retries:
            log.warning('Retrying (%i) %s', retries, boto_function)

        try:
            return boto_function(**kwargs)
        except botocore.exceptions.ClientError as exc:
            if retries >= max_retries:
                raise exc
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code == 'ProvisionedThroughputExceededException':
                time.sleep(2 ** retries * .1)
                retries += 1
            else:
                raise exc


class Client(object):
    """Synchronous Kinesis client."""

    def __init__(self, config):
        self.stream = config['stream_name']
        self.max_retries = config['kinesis_max_retries']
        self.connection = get_connection(config['aws_region'])

    def put_record(self, record):
        """Send records to Kinesis API.

        Records is a list of tuple like (data, partition_key).
        """
        data, partition_key = record

        log.debug('Sending record: %s', data[:100])
        try:
            call_and_retry(self.connection.put_record, self.max_retries,
                           StreamName=self.stream, Data=data,
                           PartitionKey=partition_key)
        except:
            log.exception('Failed to send records to Kinesis')

    def close(self):
        log.debug('Closing client')

    def join(self):
        log.debug('Joining client')


class ThreadPoolClient(Client):
    """Thread pool based asynchronous Kinesis client."""

    def __init__(self, config):
        super(ThreadPoolClient, self).__init__(config)
        self.pool = ThreadPool(processes=config['kinesis_concurrency'])

    def put_record(self, records):
        task_func = super(ThreadPoolClient, self).put_record
        self.pool.apply_async(task_func, args=[records])

    def close(self):
        super(ThreadPoolClient, self).close()
        self.pool.close()

    def join(self):
        super(ThreadPoolClient, self).join()
        self.pool.join()
