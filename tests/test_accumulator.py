import time

from kinesis_producer.accumulator import RecordAccumulator
from kinesis_producer.buffer import RawBuffer


CONFIG = {
    'buffer_time_limit': 0.1,
    'buffer_size_limit': 100,
    'record_delimiter': 'X',
}


def test_append():
    acc = RecordAccumulator(RawBuffer, CONFIG)
    success = acc.try_append('-')
    assert success

    acc.flush()
    success = acc.try_append('-')
    assert success


def test_append_over_buffer_size():
    acc = RecordAccumulator(RawBuffer, CONFIG)
    success = acc.try_append('-' * 200)
    assert success
    assert acc.is_ready()


def test_append_over_kinesis_record_size():
    acc = RecordAccumulator(RawBuffer, CONFIG)
    success = acc.try_append('-' * (1024 * 1024))
    assert not success

    acc.flush()
    success = acc.try_append('-')
    assert success


def test_append_timeout():
    acc = RecordAccumulator(RawBuffer, CONFIG)
    acc.try_append('-')
    time.sleep(0.2)
    assert acc.is_ready()

    acc.flush()
    assert not acc.is_ready()


def test_append_empty_timeout():
    acc = RecordAccumulator(RawBuffer, CONFIG)
    time.sleep(0.2)
    assert not acc.is_ready()


def test_has_record():
    acc = RecordAccumulator(RawBuffer, CONFIG)
    assert not acc.has_records()

    acc.try_append('-')
    assert acc.has_records()

    acc.flush()
    assert not acc.has_records()

    acc.try_append('-')
    assert acc.has_records()


def test_flush():
    acc = RecordAccumulator(RawBuffer, CONFIG)
    acc.try_append('123')
    acc.try_append('456')
    acc.try_append('789')
    assert acc.flush() == '123X456X789X'

    acc.try_append('ABC')
    assert acc.flush() == 'ABCX'
