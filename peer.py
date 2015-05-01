#!/usr/bin/env python

import logging

from worker import Worker
from driver import Driver
from helper import *
from cli import CLI


logging.basicConfig(level=logging.WARN, filename='peer.log', filemode='a')


class Context(object):
    def __init__(self, workers):
        self.workers = workers


worker = Worker()
driver = Driver()
bind_signal_handler(worker)
bind_signal_handler(driver)
# worker.join()
# driver.join()

context = Context(driver.workers)
# run_console({'context':context})
CLI(locals={'context':context})
