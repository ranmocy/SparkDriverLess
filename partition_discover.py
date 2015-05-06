import atexit
import logging
from collections import deque
import zerorpc
from broadcast import Discover, PARTITION_DISCOVER_TYPE

__author__ = 'ranmocy'
logger = logging.getLogger(__name__)


class PartitionDiscover():
    def __init__(self):
        partitions = deque()
        self.partitions = partitions

        def add_service(zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            logger.debug("Service %s added, service info: %s" % (name, info))

            address = info.properties['address']
            uuid = info.properties['uuid']
            if uuid not in partitions:
                partitions[uuid] = deque()
            partitions[uuid].append(address)

        def remove_service(zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            logger.debug("Service %s removed, service info: %s" % (name, info))

            address = info.properties['address']
            uuid = info.properties['uuid']
            if uuid in partitions:
                if address in partitions[uuid]:
                    partitions[uuid].remove(address)

        scanner = Discover(type=PARTITION_DISCOVER_TYPE,
                           add_service_func=add_service,
                           remove_service_func=remove_service)
        atexit.register(lambda: scanner.close())

    def get_partition(self, uuid):
        if uuid in self.partitions:
            for address in self.partitions[uuid]:
                c = zerorpc.Client()
                c.connect(address)
                try:
                    return c.fetch_partition(uuid)
                except zerorpc.RemoteError, zerorpc.LostRemote:
                    continue
        return None