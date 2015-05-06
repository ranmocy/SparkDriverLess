import logging

__author__ = 'ranmocy'
logger = logging.getLogger(__name__)


class PartitionServer():
    # - TODO: if it's taken, set a timer.
    #     - If timeout and no result, broadcast again since that worker is too slow.
    def __init__(self):
        pass