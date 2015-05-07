import logging
import atexit
from collections import deque

import gevent

from broadcast import Discover, JOB_DISCOVER_TYPE


__author__ = 'ranmocy'
logger = logging.getLogger(__name__)


class JobDiscover():
    def __init__(self):
        jobs = {}
        self.jobs = jobs
        queue = deque()
        self.queue = queue

        def add_service(zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            jobs[name] = info.properties
            queue.append(name)
            logger.debug("Job %s discovered at %s" % (name, info.properties['address']))

        def remove_service(zeroconf, type, name):
            if name in jobs:
                del jobs[name]
            if name in queue:
                queue.remove(name)
            logger.debug("Job %s removed." % name)

        scanner = Discover(type=JOB_DISCOVER_TYPE,
                           add_service_func=add_service,
                           remove_service_func=remove_service)
        atexit.register(lambda: scanner.close())

    def take_next_job_partition(self):
        while True:
            try:
                service_name = self.queue.popleft()
                return self.jobs[service_name]
            except IndexError:
                gevent.sleep(0.1)
                continue