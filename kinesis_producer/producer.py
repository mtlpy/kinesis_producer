import logging

import six
from six.moves import queue

from .sender import Sender
from .accumulator import RecordAccumulator
from .buffer import RawBuffer
from .client import Client
from .partitioner import random_partitioner
from .constants import KINESIS_RECORD_MAX_SIZE
from .exceptions import InvalidRecord

log = logging.getLogger(__name__)


class KinesisProducer(object):
    """A Kinesis client that publishes records to a Kinesis stream."""

    def __init__(self, config):
        log.debug('Starting KinesisProducer')
        self._queue = queue.Queue()

        accumulator = RecordAccumulator(RawBuffer, config)
        client = Client(config)
        self._sender = Sender(queue=self._queue,
                              accumulator=accumulator,
                              client=client,
                              partitioner=random_partitioner)
        self._sender.daemon = True
        self._sender.start()
        self._closed = False

    def send(self, record):
        """Publish a record to Kinesis.

        Don't block. Record must be bytes type.
        """
        assert not self._closed, "KinesisProducer stopped but called anyway"

        if not isinstance(record, six.binary_type):
            raise InvalidRecord("Record must be bytes type")

        if len(record) > KINESIS_RECORD_MAX_SIZE:
            raise InvalidRecord("Record is larger than max record size")

        self._queue.put(record)

    def stop(self):
        if self._closed:
            return
        log.debug('Closing KinesisProducer')
        self._sender.close()
        self._closed = True

    def join(self):
        self.stop()
        log.debug('Joining KinesisProducer')
        self._queue.join()
        log.debug('KinesisProducer record queue was joined')
        self._sender.join()
