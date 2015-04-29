import StringIO
import pickle

import gevent
import zerorpc

from rdd import *

class Driver(object):
    def __init__(self):
        super(Driver, self).__init__()

    def run(self):
        f = TextFile('myfile')\
        .map(lambda s: s.split())\
        .filter(lambda a: int(a[1]) > 2)

        objstr = f.dump()

        c = zerorpc.Client()
        c.connect("tcp://127.0.0.1:4242")

        print c.hello(objstr)


if __name__ == '__main__':
    Driver().run()
