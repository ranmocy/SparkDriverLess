#!/usr/bin/env python

import logging
import socket
import sys
from time import sleep

from zeroconf import ServiceBrowser, Zeroconf, ServiceInfo


DEFAULT_TYPE = '_http._tcp.local.'
logger = logging.getLogger(__name__)


class Service(object):

    def __init__(self, type=DEFAULT_TYPE, name='SparkP2P',
                 address=socket.inet_aton("127.0.0.1"), port=9999,
                 properties=None, server=None):
        self.type = type
        self.name = name + type # `name` must end with `type`
        self.address = address
        self.port = port
        self.properties = properties
        self.server = server
        self.zeroconf = Zeroconf()
        self.info = ServiceInfo(self.type, self.name, self.address, self.port,
                                0, 0, self.properties, self.server)
        self.zeroconf.register_service(self.info)
        logger.debug('Register '+self.name)

    def close(self):
        if self.info:
            logger.debug('unregister')
            self.zeroconf.unregister_service(self.info)
        self.zeroconf.close()


class Discover(object):

    class DiscoverListener(object):
        def remove_service(self, zeroconf, type, name):
            logger.debug("Service %s removed" % (name,))

        def add_service(self, zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            logger.debug("Service %s added, service info: %s" % (name, info))

    def __init__(self, listener=DiscoverListener()):
        self.zeroconf = Zeroconf()
        self.listener = listener
        self.browser = ServiceBrowser(self.zeroconf, DEFAULT_TYPE, self.listener)

    def close(self):
        self.zeroconf.close()
