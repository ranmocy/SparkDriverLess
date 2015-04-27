#!/usr/bin/env python

import StringIO
import pickle

import gevent
import zerorpc


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

s = zerorpc.Server(Worker())
s.bind("tcp://0.0.0.0:4242")
print 'running'
s.run()
