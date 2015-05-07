import atexit
import logging
import uuid as uuid_lib

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
        if uuid not in self.services:  # partition is missing
            print 'Missing partition', uuid, self.services
            return None
        print 'return result:' + str(self.services[uuid].result)
        return dump(self.services[uuid].result)


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
        if uuid in self.services:
            logger.warning('duplicated partition service:' + uuid)
            return
        name = 'Spark_Partition_' + str(uuid_lib.uuid4())
        properties = {'name': name, 'uuid': uuid, 'address': self.address}
        service = Service(name=name, type=PARTITION_DISCOVER_TYPE, port=self.port, properties=properties)
        service.result = result  # attach additional information for handler
        self.services[uuid] = service
        logger.info('add partition service:' + uuid + ' at ' + self.address)

    def remove(self, uuid):
        if uuid in self.services:
            self.services[uuid].close()
            del self.services[uuid]