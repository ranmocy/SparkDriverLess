#!/usr/bin/env python

import subprocess

from worker import Worker
from driver import Driver
from helper import *
from colors import success, warn


worker = Worker()
driver = Driver()
bind_signal_handler(worker)
bind_signal_handler(driver)
worker.join()
driver.join()
