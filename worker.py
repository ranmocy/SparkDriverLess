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


# 1. broadcast a `worker` with new generated uuid, {address=ip:port}
# 2. discover `job`, append to jobs
# 3. discover `rdd`, append to rdds
# 4. start a result server
# 5. start a partions_server
#     - TODO: if it's taken, set a timer.
#     - If timout and no result, broadcast again since that worker is too slow.
# 6. start a loop keep trying to get a job from jobs:
#     1. connect to job's source, lock it up to prevent other workers to take it
#     2. get the dumped_rdd, unload it
#     3. run the target_rdd
#         - if narrow_dependent:
#             - do it right away
#         - if wide_dependent:
#             - try search dep_rdd in rdds
#                 - if exists, fetch result from corresponding worker
#                 - if doesn't exist, or previous try failed
#                     1. for every partion of dep_rdd:
#                         1. append to partions_server
#                         2. broadcast a `job` with partition uuid
#                     2. append current job back to jobs
#                     3. DONT sleep(NETWORK_LATENCY * 2). it's better to it locally to avoid network transfer
#                     3. continue to next job
#     4. add result to the result server
#     5. broadcast this rdd

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
