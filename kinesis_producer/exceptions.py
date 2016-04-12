
class KinesisProducerError(Exception):
    pass


class InvalidRecord(KinesisProducerError):
    pass
