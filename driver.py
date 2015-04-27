#!/usr/bin/env python

import zerorpc
from rdd import *


f = TextFile('myfile')\
.map(lambda s: s.split())\
.filter(lambda a: int(a[1]) > 2)

objstr = f.dump()

c = zerorpc.Client()
c.connect("tcp://127.0.0.1:4242")

print c.hello(objstr)

