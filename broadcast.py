#!/usr/bin/env python
from collections import deque
import logging

import gevent

from minusconf import Advertiser, Seeker
from minusconf import Service as ConfService
from helper import singleton


WORKER_DISCOVER_TYPE = '_spark.worker.'
PARTITION_DISCOVER_TYPE = '_spark.partition.'
logger = logging.getLogger(__name__)


@singleton
class Broadcaster(object):
    def __init__(self, name='SparkDriverLessBroadcaster'):
        self.name = name
        self.services = deque()
        self.advertiser = Advertiser(self.services, self.name)
        self.thread = gevent.spawn(self.advertiser.run)
        logger.debug("Broadcaster is started.")

    def __del__(self):
        self.thread.kill()
        logger.debug("Broadcaster is closed.")

    def add(self, service):
        if service.conf_service not in self.services:
            self.services.append(service.conf_service)
            logger.debug("Service "+service.type+" added:"+service.name)

    def remove(self, service):
        if service.conf_service in self.services:
            self.services.remove(service.conf_service)
            logger.debug("Service removed:"+service.name)


class Service(object):
    def __init__(self, type='spark.driver-less', name='SparkDriverLess', location="0.0.0.0", port=9999):
        self.type = type
        self.name = name
        self.location = location
        self.port = port
        self.active = True
        self.conf_service = ConfService(stype=self.type, port=self.port, sname=self.name, location=self.location)

    def is_active(self):
        return self.active

    def activate(self):
        self.active = True

    def deactivate(self):
        self.active = False


class Discover(object):
    def __init__(self, type='', advertiser_name='', service_name='', found_func=None, error_func=None):
        self.results = {}  # uuid => set(results)

        discover = self

        def found(seeker, result):
            for uuid in discover.results:
                if result.uuid == uuid:
                    return
            logger.info("Found "+result.type+":"+result.sname+" at "+result.address)

        def on_error(*args, **kwargs):
            logger.error('on Discover', args, kwargs)

        self.seeker = Seeker(stype=type, aname=advertiser_name, sname=service_name, timeout=1.0,
                             find_callback=found_func or found, error_callback=error_func or on_error)
        self.thread = gevent.spawn(self.run_forever)
        logger.debug("Discover started.")

    def __del__(self):
        self.thread.kill()
        logger.debug("Discover closed.")

    def run_forever(self):
        while True:
            results = self.seeker.run()
            origin_results = set(self.results.values())
            results_to_add = results - origin_results
            results_to_remove = origin_results - results

            # remove first
            for result in results_to_remove:
                uuid = result.uuid
                self.results[uuid].remove(result)
                if len(self.results[uuid]) is 0:
                    del self.results[uuid]

            # then add missing
            for result in results_to_add:
                uuid = result.uuid
                if uuid not in self.results:
                    self.results[uuid] = set()
                self.results[uuid].add(result)

            gevent.sleep(0)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, filename='worker.log', filemode='a')
    import sys
    if sys.argv[1] == 'a':
        s = Service(type='stype')
        s.broadcaster.thread.join()
    else:
        d = Discover(type='stype')
        d.thread.join()
