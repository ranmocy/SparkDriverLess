#!/usr/bin/env python

import sys
import signal

import gevent

from colors import warn
from worker import Worker
from driver import Driver
from broadcast import Service, Discover


worker = Worker()
worker_thread = gevent.spawn(worker.run, 9999)
service = Service()

driver = Driver()
driver_thread = gevent.spawn(driver.run)
discover = Discover()


def signal_handler(sig, frame):
    try:
        print warn("Catch "+str(sig))
        service.close()
        discover.close()
        sys.exit()
    except Exception, e:
        print e
        sys.exit()

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

gevent.joinall([worker_thread, driver_thread])
