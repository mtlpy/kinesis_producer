import logging
import time

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
            log.warning('Retrying %s', boto_function)

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

    def put_records(self, records):
        """Send records to Kinesis API.

        Records is a list of tuple like (data, partition_key).
        """
        records = [{'Data': r, 'PartitionKey': p} for r, p in records]

        log.debug('Sending records: %s', str(records)[:100])
        try:
            call_and_retry(self.connection.put_records, self.max_retries,
                           StreamName=self.stream, Records=records)
        except:
            log.exception('Failed to send records to Kinesis')

    def close(self):
        log.debug('Closing client')

    def join(self):
        log.debug('Joining client')


class DummyClient(object):

    def __init__(self, delay):
        self.delay = delay

    def put_records(self, records):
        time.sleep(self.delay)

    def close(self):
        log.debug('Closing client')

    def join(self):
        log.debug('Joining client')
