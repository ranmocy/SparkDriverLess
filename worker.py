import logging
import socket
import sys
import subprocess
import StringIO
import pickle
import uuid

import gevent
import zerorpc

from colors import *
from helper import *
from broadcast import Service
from rdd import *


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)


class Worker(object):

    def __init__(self, uuid=str(uuid.uuid1()), ip=get_my_ip(), port=get_open_port()):
        self.uuid = uuid
        self.ip = ip
        self.port = port
        self.address = get_my_address(port=self.port)
        self.server = zerorpc.Server(self)
        self.server.bind(self.address)
        self.thread = gevent.spawn(self.server.run)
        self.service = Service(name='SparkP2P_'+self.uuid, port=self.port, properties=self.get_properties())
        logger.info('Worker '+self.uuid+' is running at '+self.address)

    def __del__(self):
        self.service.close()
        logger.info('service closed')

    def get_properties(self):
        return {'uuid': self.uuid, 'address': self.address}

    def join(self):
        self.thread.join()

    def load(self, obj_str):
        string_io = StringIO.StringIO(obj_str)
        unpickler = pickle.Unpickler(string_io)
        return unpickler.load()

    def hello(self):
        logger.info('Hello')
        return 'Alive'

    def run(self, obj_str):
        return str(self.load(obj_str).collect())


if __name__ == '__main__':
    worker = Worker()
    bind_signal_handler(worker)
    worker.join()
