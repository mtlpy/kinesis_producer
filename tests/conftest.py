import boto3

from moto import mock_kinesis
import pytest


TEST_STREAM_NAME = 'STREAM_NAME'


@pytest.fixture(scope="module")
def config():
    config = dict(
        buffer_size_limit=100,
        buffer_time_limit=0.2,
        record_delimiter='\n',
        stream_name=TEST_STREAM_NAME,
        kinesis_max_retries=3,
        aws_region='us-east-1',
    )
    return config


@pytest.fixture()
def clean_boto_configuration(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'AK000000000000000000')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY',
                       '0000000000000000000000000000000000000000')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    # Reset previously created default session (for credentials)
    boto3.setup_default_session()


class StreamTestHelper(object):

    def __init__(self, stream_name):
        self.stream_name = stream_name

    def read_records_from_stream(self):
        conn = boto3.client('kinesis')
        stream = conn.describe_stream(StreamName=self.stream_name)
        shard_id = stream['StreamDescription']['Shards'][0]['ShardId']
        iterator = conn.get_shard_iterator(StreamName=self.stream_name,
                                           ShardId=shard_id,
                                           ShardIteratorType='TRIM_HORIZON')
        records = conn.get_records(ShardIterator=iterator['ShardIterator'])
        return records['Records']


@pytest.fixture()
def kinesis(request, clean_boto_configuration):
    mock = mock_kinesis()
    mock.start()

    conn = boto3.client('kinesis')
    conn.create_stream(StreamName=TEST_STREAM_NAME, ShardCount=1)

    def teardown():
        mock.stop()
    request.addfinalizer(teardown)

    return StreamTestHelper(TEST_STREAM_NAME)
