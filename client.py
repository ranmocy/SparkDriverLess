#!/usr/bin/env python
import atexit
from collections import deque
import logging

from broadcast import Discover, WORKER_DISCOVER_TYPE, RDD_DISCOVER_TYPE
from rdd import Context
from cli import CLI


logging.basicConfig(level=logging.INFO, filename='client.log', filemode='a')
logger = logging.getLogger(__name__)


def discover_workers(worker_list):
    def add_service(zeroconf, type, name):
        info = zeroconf.get_service_info(type, name)
        logger.debug("Service %s added, service info: %s" % (name, info))
        worker_list.append(info.properties['address'])

    def remove_service(zeroconf, type, name):
        info = zeroconf.get_service_info(type, name)
        logger.debug("Service %s removed, service info: %s" % (name, info))
        worker_list.remove(info.properties['address'])

    scanner = Discover(type=WORKER_DISCOVER_TYPE, add_service_func=add_service, remove_service_func=remove_service)
    atexit.register(lambda: scanner.close())


def discover_rdds(rdd_list):
    def add_service(zeroconf, type, name):
        info = zeroconf.get_service_info(type, name)
        logger.debug("Service %s added, service info: %s" % (name, info))
        address = info.properties['address']
        uuid = info.properties['uuid']
        if uuid not in rdd_list:
            rdd_list[uuid] = deque()
        rdd_list[uuid].append(address)

    def remove_service(zeroconf, type, name):
        info = zeroconf.get_service_info(type, name)
        logger.debug("Service %s removed, service info: %s" % (name, info))
        address = info.properties['address']
        uuid = info.properties['uuid']
        if uuid in rdd_list:
            if address in rdd_list[uuid]:
                rdd_list[uuid].remove(address)

    scanner = Discover(type=RDD_DISCOVER_TYPE, add_service_func=add_service, remove_service_func=remove_service)
    atexit.register(lambda: scanner.close())


class PartitionServer():
    # - TODO: if it's taken, set a timer.
    #     - If timeout and no result, broadcast again since that worker is too slow.
    def __init__(self):
        pass


if __name__ == '__main__':
    # 1. discover `worker`, append to workers
    workers = deque()
    discover_workers(workers)

    # 2. discover `rdd`, append to rdds
    rdds = deque()
    discover_rdds(rdds)

    # 3. start partitions_server
    partition_server = PartitionServer()

    # 4. start console for the user
    CLI(local={'context': Context()})
