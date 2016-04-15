import mock
import pytest

import botocore.exceptions
from kinesis_producer.client import Client, call_and_retry


def test_init(kinesis):
    config = {
        'aws_region': 'us-east-1',
        'stream_name': 'STREAM_NAME',
        'kinesis_max_retries': 3,
    }
    Client(config)


def test_close(kinesis, config):
    c = Client(config)
    c.close()
    c.join()


def test_send_record(kinesis):
    config = {
        'aws_region': 'us-east-1',
        'stream_name': 'STREAM_NAME',
        'kinesis_max_retries': 3,
    }
    client = Client(config)

    record = (b'data', 'part')
    client.put_record(record)

    records = kinesis.read_records_from_stream()

    assert len(records) == 1
    assert records[0]['PartitionKey'] == 'part'
    assert records[0]['Data'] == b'data'


def test_send_records_handle_error(config, kinesis):
    client = Client(config)

    record = (b'data', 'part1')

    with mock.patch.object(client, 'connection') as m_conn:
        m_conn.put_record.side_effect = Exception()

        client.put_record(record)


def test_retry_logic_call():
    func = mock.Mock()
    resp = call_and_retry(func, 3, arg='ARG')

    assert resp == func.return_value
    func.assert_called_once_with(arg='ARG')


def test_retry_logic_unknown_exception():
    func = mock.Mock()
    func.side_effect = Exception()

    with pytest.raises(Exception):
        call_and_retry(func, 3, arg='ARG')


def test_retry_logic_client_error():
    error = {'Error': {'Code': 'SomeError'}}
    exc = botocore.exceptions.ClientError(error, None)

    func = mock.Mock()
    func.side_effect = exc

    with pytest.raises(botocore.exceptions.ClientError):
        call_and_retry(func, 3, arg='ARG')


def test_retry_logic_throughput_error():
    error = {'Error': {'Code': 'ProvisionedThroughputExceededException'}}
    exc = botocore.exceptions.ClientError(error, None)

    func = mock.Mock()
    func.side_effect = [exc, exc, 'RESPONSE']

    resp = call_and_retry(func, 2, arg='ARG')

    assert resp == 'RESPONSE'


def test_retry_logic_throughput_error_give_up():
    error = {'Error': {'Code': 'ProvisionedThroughputExceededException'}}
    exc = botocore.exceptions.ClientError(error, None)

    func = mock.Mock()
    func.side_effect = [exc, exc, exc]

    with pytest.raises(botocore.exceptions.ClientError):
        call_and_retry(func, 2, arg='ARG')
