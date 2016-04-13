import io

from .constants import KINESIS_RECORD_MAX_SIZE


class RawBuffer(object):
    """Bytes buffer with delimiter."""

    def __init__(self, config):
        self.record_delimiter = config['record_delimiter']
        self.size_limit = config['buffer_size_limit']
        self._size = 0
        self._buffer = io.BytesIO()

    def try_append(self, record):
        """Append a record if possible, return False otherwise."""
        assert self._buffer is not None, 'Buffer is closed!'

        record_length = len(record) + len(self.record_delimiter)

        if self._size + record_length > KINESIS_RECORD_MAX_SIZE:
            return False

        self._buffer.write(record)
        self._buffer.write(self.record_delimiter)
        self._size += record_length
        return True

    def is_ready(self):
        """Whether the buffer should be flushed."""
        return self._size > self.size_limit

    def flush(self):
        """Return the buffer content and close the buffer."""
        assert self._buffer is not None, 'Buffer is closed!'
        buf = self._buffer.getvalue()
        self._buffer = None
        return buf
