import atexit
import logging

import gevent
import zerorpc

from broadcast import Service, PARTITION_DISCOVER_TYPE
from helper import get_my_ip, get_open_port, get_my_address, dump


__author__ = 'ranmocy'
logger = logging.getLogger(__name__)


class PartitionServerHandler(object):
    def __init__(self, services, address=get_my_address(get_open_port())):
        self.services = services
        self.address = address
        self.server = zerorpc.Server(self)
        self.server.bind(self.address)
        self.thread = gevent.spawn(self.server.run)
        logger.info("Job server started at " + self.address)

    def __del__(self):
        self.thread.kill()

    def fetch_partition(self, uuid):
        name = service_name(uuid)
        if name not in self.services:  # partition is missing
            print 'Missing partition', name, self.services
            return None
        print 'return result:' + str(self.services[name].result)
        return dump(self.services[name].result)


def service_name(uuid):
    return 'Spark_Partition_' + uuid


class PartitionServer(object):
    def __init__(self):
        self.ip = get_my_ip()
        self.port = get_open_port()
        self.address = get_my_address(port=self.port)
        self.services = {}
        self.handler = PartitionServerHandler(self.services, address=self.address)
        atexit.register(lambda: self.__del__())

    def __del__(self):
        for service in self.services:
            service.close()
        self.handler.__del__()

    def add(self, uuid, result):
        name = service_name(uuid)
        if name in self.services:
            logger.warning('duplicated partition service:' + name)
            return
        properties = {'name': name, 'uuid': uuid, 'address': self.address}
        service = Service(name=name, type=PARTITION_DISCOVER_TYPE, port=self.port, properties=properties)
        service.result = result  # attach additional information for handler
        self.services[name] = service
        logger.info('add partition service:' + name + ' at ' + self.address)

    def remove(self, uuid):
        name = service_name(uuid)
        if name in self.services:
            self.services[name].close()
            del self.services[name]