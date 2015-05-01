#!/usr/bin/env python

import logging

from worker import Worker
from driver import Driver
from rdd import Context
from helper import *
from cli import CLI


logging.basicConfig(level=logging.WARN, filename='peer.log', filemode='a')


worker = Worker()
driver = Driver()

def close_all():
    worker.close()
    driver.close()

import atexit
atexit.register(close_all)
del atexit

CLI(local={'context':Context(worker=worker, driver=driver)})
