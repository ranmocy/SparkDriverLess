import atexit
import logging
from collections import deque
from broadcast import Discover, WORKER_DISCOVER_TYPE

__author__ = 'ranmocy'
logger = logging.getLogger(__name__)


class WorkerDiscover():
    def __init__(self):
        workers = deque()
        self.workers = workers

        def add_service(zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            logger.debug("Service %s added, service info: %s" % (name, info))
            workers.append(info.properties['address'])

        def remove_service(zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            logger.debug("Service %s removed, service info: %s" % (name, info))
            workers.remove(info.properties['address'])

        scanner = Discover(type=WORKER_DISCOVER_TYPE,
                           add_service_func=add_service,
                           remove_service_func=remove_service)
        atexit.register(lambda: scanner.close())