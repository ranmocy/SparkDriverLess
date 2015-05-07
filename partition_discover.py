import atexit
import logging
from collections import deque

import zerorpc

from broadcast import Discover, PARTITION_DISCOVER_TYPE
from helper import load


__author__ = 'ranmocy'
logger = logging.getLogger(__name__)


class PartitionDiscover():
    def __init__(self):
        partitions = {}
        self.partitions = partitions

        def add_service(zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            if info:
                uuid = info.properties['uuid']
                if uuid not in partitions:
                    partitions[uuid] = deque()
                partitions[uuid].append(info.properties)
                logger.debug("Partition %s added" % name)

        def remove_service(zeroconf, type, name):
            logger.debug("Partition %s removed" % name)

            for uuid, services in enumerate(partitions):
                target_service = None
                for service in services:
                    if service['name'] == name:
                        target_service = service
                        break
                if target_service:
                    services.remove(target_service)

        scanner = Discover(type=PARTITION_DISCOVER_TYPE,
                           add_service_func=add_service,
                           remove_service_func=remove_service)
        atexit.register(lambda: scanner.close())

    def get_partition(self, uuid):
        if uuid in self.partitions:
            # create a new list to prevent iteration error
            for service in list(self.partitions[uuid]):
                address = service['address']
                c = zerorpc.Client()
                c.connect(address)
                try:
                    result = load(c.fetch_partition(uuid))
                    print 'got partition result:'+str(result)
                    return result
                except zerorpc.RemoteError, zerorpc.LostRemote:
                    continue
        return None