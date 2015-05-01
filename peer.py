#!/usr/bin/env python

import subprocess
import sys
import code

from gevent import fileobject

from worker import Worker
from driver import Driver
from helper import *
from colors import success, warn


logging.basicConfig(level=logging.WARN, filename='peer.log', filemode='a')

_green_stdin = fileobject.FileObject(sys.stdin)
_green_stdout = fileobject.FileObject(sys.stdout)

def _green_raw_input(prompt):
    _green_stdout.write(prompt)
    return _green_stdin.readline()[:-1]

def run_console(local=None, prompt=">>>"):
    code.interact(prompt, _green_raw_input, local=local or {})


worker = Worker()
driver = Driver()
bind_signal_handler(worker)
bind_signal_handler(driver)
# worker.join()
# driver.join()

run_console()
