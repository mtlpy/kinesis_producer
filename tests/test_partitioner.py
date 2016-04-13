import six

from kinesis_producer.partitioner import random_partitioner


def test_random_key_is_string():
    key = random_partitioner(None)
    assert isinstance(key, six.string_types)


def test_random_key_changes():
    key1 = random_partitioner(None)
    key2 = random_partitioner(None)
    key3 = random_partitioner(None)
    assert key1 != key2 != key3
