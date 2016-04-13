import pytest

from kinesis_producer.buffer import RawBuffer

CONFIG = {
    'record_delimiter': b'X',
    'buffer_size_limit': 100,
}


def test_append():
    buf = RawBuffer(CONFIG)

    buf.try_append(b'123')
    buf.try_append(b'456')
    buf.try_append(b'789')

    value = buf.flush()

    assert value == b'123X456X789X'


def test_is_ready():
    buf = RawBuffer(CONFIG)

    buf.try_append(b'-' * 98)  # + delimiter == 99 bytes

    assert not buf.is_ready()

    buf.try_append(b'-')

    assert buf.is_ready()


def test_try_append_response():
    buf = RawBuffer(CONFIG)

    success = buf.try_append(b'-')
    assert success

    # Over buffer_size_limit
    msg = b'-' * 1024
    success = buf.try_append(msg)
    assert success

    # Over Kinesis record limit
    msg = b'-' * (1024 * 1023)
    success = buf.try_append(msg)
    assert not success


def test_closed():
    buf = RawBuffer(CONFIG)
    buf.flush()

    with pytest.raises(AssertionError):
        buf.try_append(b'-')

    with pytest.raises(AssertionError):
        buf.flush()
