import Queue

import mock

from kinesis_producer.sender import Sender
from kinesis_producer.accumulator import RecordAccumulator
from kinesis_producer.buffer import RawBuffer


def partitioner(record):
    return 4  # chosen by fair dice roll. garanteed to be random.


def test_init(config):
    queue = Queue.Queue()
    accumulator = RecordAccumulator(RawBuffer, config)
    client = mock.Mock()

    sender = Sender(queue=queue, accumulator=accumulator,
                    client=client, partitioner=partitioner)
    sender.start()
    sender.close()
    sender.join()


def test_flush(config):
    queue = Queue.Queue()
    accumulator = RecordAccumulator(RawBuffer, config)
    client = mock.Mock()

    sender = Sender(queue=queue, accumulator=accumulator,
                    client=client, partitioner=partitioner)

    sender.flush()
    assert not client.put_records.called

    accumulator.try_append('-')

    sender.flush()
    expected_records = [('-\n', 4)]
    client.put_records.assert_called_once_with(expected_records)


def test_accumulate(config):
    queue = Queue.Queue()
    accumulator = RecordAccumulator(RawBuffer, config)
    client = mock.Mock()

    sender = Sender(queue=queue, accumulator=accumulator,
                    client=client, partitioner=partitioner)

    sender.run_once()
    assert not accumulator.has_records()

    queue.put('-')

    sender.run_once()
    assert accumulator.has_records()


def test_flush_if_ready(config):
    queue = Queue.Queue()
    accumulator = RecordAccumulator(RawBuffer, config)
    client = mock.Mock()

    sender = Sender(queue=queue, accumulator=accumulator,
                    client=client, partitioner=partitioner)

    accumulator.try_append('-' * 200)
    sender.run_once()

    assert client.put_records.called
    assert not accumulator.has_records()


def test_flush_if_full(config):
    queue = Queue.Queue()
    accumulator = RecordAccumulator(RawBuffer, config)
    client = mock.Mock()

    sender = Sender(queue=queue, accumulator=accumulator,
                    client=client, partitioner=partitioner)

    accumulator.try_append('-' * (1024 * 1024 - 1))
    queue.put('-' * 50)
    sender.run_once()

    assert client.put_records.called
    assert accumulator.has_records()
