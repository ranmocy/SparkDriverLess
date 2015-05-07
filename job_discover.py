import StringIO
import logging
import atexit
from collections import deque

import gevent
import pickle
import zerorpc

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

    def load(self, obj_str):
        string_io = StringIO.StringIO(obj_str)
        unpickler = pickle.Unpickler(string_io)
        return unpickler.load()

    def take_next_job_partition(self):
        while True:
            try:
                service_name = self.queue.popleft()
                job = self.jobs[service_name]
                name = job['name']
                address = job['address']
                logger.info('take job:' + address)
                c = zerorpc.Client()
                c.connect(address)
                obj_str = c.take(name)
                return self.load(obj_str)
            except IndexError:
                gevent.sleep(0.1)
                continue