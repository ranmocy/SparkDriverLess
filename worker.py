import atexit
import logging
import uuid
import gevent

from helper import get_my_ip, get_my_address, get_open_port
from broadcast import Service
from job_discover import JobDiscover
from partition_discover import PartitionDiscover
from result_server import ResultServer


logger = logging.getLogger(__name__)


class Worker(object):

    def __init__(self, uuid=str(uuid.uuid1()), ip=get_my_ip(), port=get_open_port()):
        self.uuid = uuid
        self.ip = ip
        self.port = port
        self.address = get_my_address(port=self.port)
        self.service = Service(name='SparkDriverLess_'+self.uuid, port=self.port, properties=self.get_properties())
        atexit.register(lambda: self.__del__())
        logger.info('Worker '+self.uuid+' is running at '+self.address)

    def __del__(self):
        self.service.close()

    def get_properties(self):
        return {'uuid': self.uuid, 'address': self.address}

    def hello(self):
        logger.info('Hello')
        return 'Alive'


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, filename='worker.log', filemode='a')

    # 1. broadcast a `worker` with new generated uuid, {address=ip:port}
    worker = Worker()
    # 2. discover `job`, append to jobs
    job_discover = JobDiscover()
    # 3. discover `partitions`, append to partitions
    partition_discover = PartitionDiscover()
    # 4. start a result server
    result_server = ResultServer()
    # 5. start a partitions_server
    # 6. start a loop keep trying to get a job from jobs:
    while True:
        print('Fetching job...')
        partition = job_discover.take_next_job_partition()  # block here
        print partition.get()
        # 1. connect to job's source, lock it up to prevent other workers to take it

        # 2. get the dumped_rdd, unload it
        # 3. run the target_rdd
        # 4. add result to the result server
        # 5. broadcast this rdd
        gevent.sleep(1)
        pass