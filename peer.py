#!/usr/bin/env python

import logging

from worker import Worker
from driver import Driver
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


class Context(object):
    def __init__(self):
        self.worker = worker
        self.driver = driver
        self.workers = driver.workers


CLI(local={'context':Context()})
