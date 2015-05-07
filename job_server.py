import atexit
import logging
import threading

import gevent
import zerorpc

from broadcast import Service, JOB_DISCOVER_TYPE
from helper import get_my_ip, get_open_port, get_my_address


__author__ = 'ranmocy'
logger = logging.getLogger(__name__)


class JobServerHandler(object):
    def __init__(self, services, address=get_my_address(get_open_port())):
        self.services = services
        self.address = address
        self.server = zerorpc.Server(self)
        self.server.bind(self.address)
        self.thread = gevent.spawn(self.server.run)
        logger.info("Job server started at " + self.address)
        self.lock = threading.Lock()

    def __del__(self):
        self.thread.kill()

    def take(self, name):
        try:
            self.lock.acquire()
            if name not in self.services:  # job is finished
                print 'finished job', name, self.services.keys()
                return None
            service = self.services[name]
            if not service.is_registered():  # job is taken
                print 'taken job'
                return None
            service.unregister()
            # - TODO: if it's taken, set a timer.
            #     - If timeout and no result, broadcast again since that worker is dead, or too slow.
            return service.partition.dump()
        finally:
            self.lock.release()


def service_name(partition):
    return 'SparkDriverLess_Job_' + partition.uuid


class JobServer(object):
    def __init__(self):
        self.ip = get_my_ip()
        self.port = get_open_port()
        self.address = get_my_address(port=self.port)
        self.services = {}
        self.handler = JobServerHandler(self.services, address=self.address)
        atexit.register(lambda: self.__del__())

    def __del__(self):
        for service in self.services:
            service.close()
        self.handler.__del__()

    def add(self, partition):
        name = service_name(partition)
        if name in self.services:
            logger.warning('duplicated job service:' + name)
            return
        properties = {'name': name, 'uuid': partition.uuid, 'address': self.address}
        service = Service(name=name, type=JOB_DISCOVER_TYPE, port=self.port, properties=properties)
        service.partition = partition  # attach additional information for handler
        self.services[name] = service
        logger.info('add job service:' + name + ' at ' + self.address)

    def remove(self, partition):
        name = service_name(partition)
        if name in self.services:
            self.services[name].close()