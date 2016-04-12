import time


class RecordAccumulator(object):

    def __init__(self, buffer_class, config):
        self.config = config
        self.buffer_time_limit = config['buffer_time_limit']
        self._buffer_class = buffer_class
        self._reset_buffer()

    def _reset_buffer(self):
        self._buffer = self._buffer_class(config=self.config)
        self._buffer_started_at = None

    def try_append(self, record):
        """Attempt to accumulate a record. Return False if buffer is full."""
        success = self._buffer.try_append(record)
        if success:
            self._buffer_started_at = time.time()
        return success

    def is_ready(self):
        """Check whether the buffer is ready."""
        if self._buffer_started_at is None:
            return False

        if self._buffer.is_ready():
            return True

        elapsed = time.time() - self._buffer_started_at
        return elapsed >= self.buffer_time_limit

    def has_records(self):
        """Check whether the buffer has records."""
        return self._buffer_started_at is not None

    def flush(self):
        """Close the buffer and return it."""
        if self._buffer_started_at is None:
            return
        buf = self._buffer.flush()
        self._reset_buffer()
        return buf
