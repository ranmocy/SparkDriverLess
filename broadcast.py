#!/usr/bin/env python

import logging
import socket

from zeroconf import ServiceBrowser, Zeroconf, ServiceInfo


DEFAULT_TYPE = '_http._tcp.local.'
WORKER_DISCOVER_TYPE = '_http._tcp.worker.'
PARTITION_DISCOVER_TYPE = '_http._tcp.partition.'
JOB_DISCOVER_TYPE = '_http._tcp.job.'
logger = logging.getLogger(__name__)


class Service(object):
    def __init__(self, type=DEFAULT_TYPE, name='SparkDriverLess',
                 address=socket.inet_aton("127.0.0.1"), port=9999,
                 properties=None, server=None):
        self.type = type
        self.name = name + type  # `name` must end with `type`
        self.address = address
        self.port = port
        self.properties = properties
        self.server = server
        self.zeroconf = Zeroconf()
        self.info = ServiceInfo(self.type, self.name, self.address, self.port,
                                0, 0, self.properties, self.server)
        self.zeroconf.register_service(self.info)
        # logger.debug('Register ' + self.name)

    def is_registered(self):
        return self.info.name.lower() in self.zeroconf.services

    def unregister(self):
        if self.info:
            logger.debug('unregister')
            self.zeroconf.unregister_service(self.info)

    def close(self):
        self.unregister()
        self.zeroconf.close()


class Discover(object):
    def __init__(self, type=DEFAULT_TYPE, add_service_func=None, remove_service_func=None):
        def add_service(zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            logger.debug("Service %s added, service info: %s" % (name, info))

        def remove_service(zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            logger.debug("Service %s removed, service info: %s" % (name, info))

        if add_service_func is None:
            add_service_func = add_service
        if remove_service_func is None:
            remove_service_func = remove_service

        class DiscoverListener(object):
            def add_service(self, zeroconf, s_type, name):
                return add_service_func(zeroconf, s_type, name)

            def remove_service(self, zeroconf, s_type, name):
                return remove_service_func(zeroconf, s_type, name)

        self.zeroconf = Zeroconf()
        self.listener = DiscoverListener()
        self.browser = ServiceBrowser(self.zeroconf, type, self.listener)

    def close(self):
        self.zeroconf.close()
