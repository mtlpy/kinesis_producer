import logging
import threading

from six.moves import queue

log = logging.getLogger(__name__)


class Sender(threading.Thread):
    """I/O thread accumulating records and flushing to client."""

    def __init__(self, queue, accumulator, client, partitioner):
        super(Sender, self).__init__()
        self.queue = queue
        self._accumulator = accumulator
        self._client = client
        self._partitioner = partitioner
        self._running = True
        self._closed = threading.Event()

    def run(self):
        while self._running:
            try:
                self.run_once()
            except Exception:
                log.exception("Uncaught error in kinesis producer I/O thread")

        log.debug("Beginning shutdown of kinesis producer I/O thread, sending"
                  " remaining records.")

        while not self.queue.empty() or self._accumulator.has_records():
            try:
                self.run_once()
            except Exception:
                log.exception("Uncaught error in kinesis producer I/O thread")

        log.debug("Accumulator is now empty, kinesis producer I/O thread can"
                  " close.")

        self._client.close()

        self._closed.set()
        log.debug("Kinesis producer I/O thread is now closed")

    def run_once(self):
        """Accumulate records and flush when accumulator is ready."""
        try:
            record = self.queue.get(timeout=0.05)
        except queue.Empty:
            record = None
        else:
            success = self._accumulator.try_append(record)
            if not success:
                self.flush()
                success = self._accumulator.try_append(record)
                assert success, "Failed to accumulate even after flushing"

            self.queue.task_done()

        is_ready = self._accumulator.is_ready()
        force_flush = not self._running and record is None

        if is_ready or force_flush:
            self.flush()

    def flush(self):
        """Get the record by flushing the accumulator and send it to client."""
        record_data = self._accumulator.flush()
        if record_data:
            log.debug('Flushing to client (length: %i)', len(record_data))
            record = (record_data, self._partitioner(record_data))
            self._client.put_record(record)

    def close(self):
        log.debug("Closing kinesis producer I/O thread")
        self._running = False

    def join(self):
        log.debug("Joining kinesis producer I/O thread")
        self._closed.wait()
        self._client.join()
