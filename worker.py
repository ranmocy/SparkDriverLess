import StringIO
import pickle

import gevent
import zerorpc

from rdd import *


class Worker(object):

    def __init__(self):
        # gevent.spawn(self.controller)
        pass

    def controller(self):
        while True:
            print "[Contoller]"
            gevent.sleep(1)

    def load(self, obj_str):
        string_io = StringIO.StringIO(obj_str)
        unpickler = pickle.Unpickler(string_io)
        return unpickler.load()

    def hello(self, obj_str):
        print 'done'
        return str(self.load(obj_str).collect())

    def run(self, port):
        s = zerorpc.Server(self)
        address = "tcp://0.0.0.0:"+str(port)
        s.bind(address)
        print 'Worker is running at '+address
        s.run()

if __name__ == '__main__':
    Worker().run()
