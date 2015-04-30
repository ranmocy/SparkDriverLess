#!/usr/bin/env python

import subprocess

from colors import success, warn


worker = subprocess.Popen(['python', 'worker.py'])
driver = subprocess.Popen(['python', 'driver.py'])
print [p.wait() for p in worker, driver] # exitcodes
