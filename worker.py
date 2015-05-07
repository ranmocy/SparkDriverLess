import atexit
import logging
import pickle

from helper import get_my_ip, get_my_address, get_open_port
from broadcast import Service
from job_discover import JobDiscover
from partition_server import PartitionServer
from rdd import *
from result_server import ResultServer


logger = logging.getLogger(__name__)


class Worker(object):

    def __init__(self, uuid=str(uuid.uuid1()), ip=get_my_ip(), port=get_open_port()):
        self.uuid = uuid
        self.ip = ip
        self.port = port
        self.address = get_my_address(port=self.port)
        self.service = Service(name='SparkP2P_'+self.uuid, port=self.port, properties=self.get_properties())
        atexit.register(lambda: self.__del__())
        logger.info('Worker '+self.uuid+' is running at '+self.address)

    def __del__(self):
        self.service.close()
        logger.info('service closed')

    def get_properties(self):
        return {'uuid': self.uuid, 'address': self.address}

    def load(self, obj_str):
        string_io = StringIO.StringIO(obj_str)
        unpickler = pickle.Unpickler(string_io)
        return unpickler.load()

    def hello(self):
        logger.info('Hello')
        return 'Alive'

    def run_code(self, obj_str):
        return str(self.load(obj_str).collect())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, filename='client.log', filemode='a')

    # 1. broadcast a `worker` with new generated uuid, {address=ip:port}
    worker = Worker()
    # 2. discover `job`, append to jobs
    job_discover = JobDiscover()
    # 3. discover `partitions`, append to partitions
    partition_discover = PartitionDiscover()
    # 4. start a result server
    result_server = ResultServer()
    # 5. start a partitions_server
    partition_server = PartitionServer()
    # 6. start a loop keep trying to get a job from jobs:
    while True:
        job = job_discover.take_next_job()  # block here
        # 1. connect to job's source, lock it up to prevent other workers to take it

        # 2. get the dumped_rdd, unload it
        # 3. run the target_rdd
        # 4. add result to the result server
        # 5. broadcast this rdd
        pass