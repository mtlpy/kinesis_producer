import time

import pytest

from kinesis_producer.producer import KinesisProducer


def test_init(kinesis, config):
    KinesisProducer(config)


def test_close(kinesis, config):
    c = KinesisProducer(config)
    c.close()
    c.join()


def test_send_invalid_record(kinesis, config):
    c = KinesisProducer(config)

    with pytest.raises(ValueError):
        c.send(123)

    with pytest.raises(ValueError):
        c.send('-' * (1024 * 1024 + 1))

    c.close()
    c.join()


def test_send_record_size_limit(kinesis, config):
    c = KinesisProducer(config)
    c.send('-' * (1024 * 1024 - 1))

    c.close()
    c.join()


def test_send_record_immediate(kinesis, config):
    c = KinesisProducer(config)
    c.send('-' * 200)
    time.sleep(0.1)  # Let the I/O thread do its job

    records = kinesis.read_records_from_stream()
    assert len(records) == 1
    assert records[0]['Data'] == '-' * 200 + '\n'

    c.close()
    c.join()


def test_send_record_linger(kinesis, config):
    c = KinesisProducer(config)
    c.send('-' * 50)
    time.sleep(0.1)  # Let the I/O thread do its job

    records = kinesis.read_records_from_stream()
    assert len(records) == 0

    time.sleep(0.2)  # Now, the accumulator should be ready

    records = kinesis.read_records_from_stream()
    assert len(records) == 1
    assert records[0]['Data'] == '-' * 50 + '\n'

    c.close()
    c.join()


def test_records_are_not_lost(kinesis, config):
    c = KinesisProducer(config)
    c.send('-')
    c.close()
    c.join()

    records = kinesis.read_records_from_stream()
    assert len(records) == 1
    assert records[0]['Data'] == '-\n'
