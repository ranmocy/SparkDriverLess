import atexit
from collections import deque
import gevent
from broadcast import Discover, JOB_DISCOVER_TYPE
from worker import logger

__author__ = 'ranmocy'


class JobDiscover():
    def __init__(self):
        jobs = deque()
        self.jobs = jobs

        def add_service(zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            logger.debug("Service %s added, service info: %s" % (name, info))
            jobs.append(info.properties['address'])

        def remove_service(zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            logger.debug("Service %s removed, service info: %s" % (name, info))
            jobs.remove(info.properties['address'])

        scanner = Discover(type=JOB_DISCOVER_TYPE,
                           add_service_func=add_service,
                           remove_service_func=remove_service)
        atexit.register(lambda: scanner.close())

    def take_next_job(self):
        while True:
            try:
                return self.jobs.popleft()
            except IndexError:
                gevent.sleep(0.1)
                continue