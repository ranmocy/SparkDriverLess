import socket
import sys
import subprocess
import StringIO
import pickle
import uuid

import gevent
import zerorpc

from rdd import *


def get_my_ip():
    return subprocess.Popen(["hostname", "-I"], stdout=subprocess.PIPE).communicate()[0].strip()

def get_open_port():
    for i in range(10):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("", 0))
            s.listen(1)
            port = s.getsockname()[1]
            s.close()
            return port
        except Exception:
            continue
    return -1


class Worker(object):

    def __init__(self, uuid=str(uuid.uuid1()), ip=get_my_ip(), port=get_open_port()):
        self.uuid = uuid
        self.ip = ip
        self.port = port
        self.address = 'tcp://'+str(self.ip)+':'+str(self.port)
        self.server = zerorpc.Server(self)
        self.server.bind(self.address)
        self.thread = gevent.spawn(self.server.run)
        print 'Worker '+self.uuid+' is running at '+self.address

    def join(self):
        self.thread.join()

    def load(self, obj_str):
        string_io = StringIO.StringIO(obj_str)
        unpickler = pickle.Unpickler(string_io)
        return unpickler.load()

    def hello(self):
        print 'Hello'
        return 'Alive'

    def run(self, obj_str):
        return str(self.load(obj_str).collect())


if __name__ == '__main__':
    Worker().join()
