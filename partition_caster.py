import atexit
import logging

import gevent
import zerorpc

from broadcast import Service, Discover, Broadcaster
from helper import get_my_ip, get_open_port, get_my_address, dump, load


__author__ = 'ranmocy'
PARTITION_DISCOVER_TYPE = '_spark.partition.'
logger = logging.getLogger(__name__)


class PartitionServerHandler(object):
    def __init__(self, partitions, address=get_my_address(get_open_port())):
        self.partitions = partitions
        self.address = address
        self.server = zerorpc.Server(self)
        self.server.bind(self.address)
        self.thread = gevent.spawn(self.server.run)
        logger.info("Job server started at " + self.address)

    def __del__(self):
        self.thread.kill()

    def fetch_partition(self, uuid):
        if uuid not in self.partitions:
            print 'Missing partition', uuid, self.partitions
            return None
        print 'return result:' + str(self.partitions[uuid].result)
        return dump(self.partitions[uuid].result)


class PartitionServer(Broadcaster):
    def __init__(self):
        super(PartitionServer, self).__init__(name='Spark.PartitionServer')
        self.ip = get_my_ip()
        self.port = get_open_port()
        self.address = get_my_address(port=self.port)
        self.partitions = {}  # uuid => service
        self.handler = PartitionServerHandler(self.partitions, address=self.address)
        atexit.register(lambda: self.__del__())

    def __del__(self):
        super(PartitionServer, self).__del__()
        self.handler.__del__()

    def add(self, uuid, result=None):
        if uuid in self.partitions:
            logger.warning('duplicated partition service:' + uuid)
            return
        if result is None:
            raise Exception("Result shouldn't be None!")
        service = Service(name=uuid, type=PARTITION_DISCOVER_TYPE, location=self.ip, port=self.port)
        service.result = result  # attach additional information for handler

        self.partitions[uuid] = service
        super(PartitionServer, self).add(service)

    def remove(self, uuid):
        if uuid in self.partitions:
            service = self.partitions[uuid]
            del self.partitions[uuid]
            super(PartitionServer, self).remove(service)


class PartitionDiscover(Discover):
    def __init__(self):
        super(PartitionDiscover, self).__init__(type=PARTITION_DISCOVER_TYPE)

    def get_partition(self, uuid):
        if uuid in self.results:
            # create a new list to prevent iteration error
            for result in list(self.results[uuid]):
                try:
                    c = zerorpc.Client()
                    c.connect(result.address)
                    result = load(c.fetch_partition(uuid))
                    print 'got partition result:'+str(result)
                    return result
                except zerorpc.RemoteError, zerorpc.LostRemote:
                    continue
        return None