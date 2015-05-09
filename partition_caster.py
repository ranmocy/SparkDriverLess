import atexit
import logging

import gevent
import zerorpc

from broadcast import Service, Discover, Broadcaster
from helper import get_my_ip, get_open_port, get_zerorpc_address, dump, load, lazy, singleton


__author__ = 'ranmocy'
_PARTITION_CASTER_TYPE = '_spark.partition.'
logger = logging.getLogger(__name__)


class PartitionServerHandler(object):
    def __init__(self, partitions, address=get_zerorpc_address(port=get_open_port())):
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
        print 'return result:' + uuid
        return dump(self.partitions[uuid].result)


class PartitionServer(Broadcaster):
    def __init__(self):
        super(PartitionServer, self).__init__(name='Spark.PartitionServer')
        self.ip = get_my_ip()
        self.port = get_open_port()
        self.address = get_zerorpc_address(port=self.port)
        self.partitions = {}  # uuid => service
        self.handler = PartitionServerHandler(self.partitions, address=self.address)
        atexit.register(lambda: self.__del__())

    def __del__(self):
        super(PartitionServer, self).__del__()
        self.handler.__del__()

    def exists(self, uuid):
        return uuid in self.partitions

    def add(self, uuid, result=None):
        if self.exists(uuid):
            logger.warning('duplicated partition service:' + uuid)
            return
        if result is None:
            raise Exception("Result shouldn't be None!")
        service = Service(name=uuid, type=_PARTITION_CASTER_TYPE, location=self.ip, port=self.port)
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
        super(PartitionDiscover, self).__init__(type=_PARTITION_CASTER_TYPE)
        self.cache = {}  # cache the result

    def get_partition(self, uuid):
        if uuid in self.cache:
            return self.cache[uuid]

        if uuid in self.results:
            # create a new list to prevent iteration error
            for result in list(self.results[uuid]):
                try:
                    c = zerorpc.Client()
                    c.connect(result.address)
                    partition_result = load(c.fetch_partition(uuid))
                    self.cache[uuid] = partition_result
                    return partition_result
                except zerorpc.RemoteError, zerorpc.LostRemote:
                    continue
        return None