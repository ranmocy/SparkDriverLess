#!/usr/bin/env python

import socket
import sys
import signal
import subprocess

from colors import success, warn
from worker import Worker
from driver import Driver
from broadcast import Service, Discover


# Helper
def get_open_port():
    port = -1
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("", 0))
            s.listen(1)
            port = s.getsockname()[1]
            s.close()
        except Exception:
            continue
        break
    return port


def get_my_ip():
    return subprocess.Popen(["hostname", "-I"], stdout=subprocess.PIPE).communicate()[0].strip()


def signal_handler(sig, frame):
    try:
        print warn("Catch " + str(sig))
        service.close()
        discover.close()
        sys.exit()
    except Exception, e:
        print e
        sys.exit()


# Worker
worker = Worker()
properties = {'address': worker.address, 'port': worker.port, 'uuid': worker.uuid}
service = Service(name='SparkP2P_'+worker.uuid, port=worker.port, properties=properties)


# Driver
driver = Driver()
class DiscoverListener(object):
    def add_service(self, zeroconf, type, name):
        info = zeroconf.get_service_info(type, name)
        driver.add_worker(info.properties['address'])
        print success("Service found: %s" % info.properties['address'])

    def remove_service(self, zeroconf, type, name):
        info = zeroconf.get_service_info(type, name)
        driver.remove_worker(info.properties['address'])
        print success("Service removed: %s" % (name,))
discover = Discover(listener=DiscoverListener())
discover.discover_service()


# Signal
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


# Join
worker.join()
driver.join()
