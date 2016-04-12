import random


def random_partitioner(stream_record):
    """Generate a random partition_key."""
    random_key = str(random.randint(0, 10**12))
    return random_key
