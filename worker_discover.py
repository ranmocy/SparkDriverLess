import atexit
import logging

from broadcast import Discover, WORKER_DISCOVER_TYPE


__author__ = 'ranmocy'
logger = logging.getLogger(__name__)


class WorkerDiscover():
    def __init__(self):
        workers = {}
        self.workers = workers

        def add_service(zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            address = info.properties['address']
            logger.debug("Worker added %s at %s." % (name, address))
            workers[name] = address

        def remove_service(zeroconf, type, name):
            logger.debug("Worker removed %s removed." % name)
            if name in workers:
                del workers[name]

        scanner = Discover(type=WORKER_DISCOVER_TYPE,
                           add_service_func=add_service,
                           remove_service_func=remove_service)
        atexit.register(lambda: scanner.close())

    def size(self):
        return len(self.workers)