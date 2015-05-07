import atexit
from collections import deque
import gevent
import zerorpc
from broadcast import Discover, JOB_DISCOVER_TYPE
from worker import logger

__author__ = 'ranmocy'


class JobDiscover():
    def __init__(self):
        jobs = {}
        self.jobs = jobs
        queue = deque()
        self.queue = queue

        def add_service(zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            logger.debug("Service %s added, service info: %s" % (name, info))
            jobs[name] = info.properties
            queue.append(name)

        def remove_service(zeroconf, type, name):
            logger.debug("Service %s removed." % name)
            if name in jobs:
                queue.remove(name)
                del jobs[name]

        scanner = Discover(type=JOB_DISCOVER_TYPE,
                           add_service_func=add_service,
                           remove_service_func=remove_service)
        atexit.register(lambda: scanner.close())

    def take_next_job_partition(self):
        while True:
            try:
                name = self.queue.popleft()
                job = self.jobs[name]
                address = job['address']
                logger.info('take job:' + address)
                c = zerorpc.Client()
                c.connect(address)
                return c.take(name)
            except IndexError:
                gevent.sleep(0.1)
                continue