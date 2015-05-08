#!/usr/bin/env python

import atexit
import logging
import uuid

import zerorpc

from helper import get_my_ip, get_my_address, get_open_port, load
from broadcast import Service, WORKER_DISCOVER_TYPE
from job_caster import JobDiscover
from partition_discover import PartitionDiscover
from partition_server import PartitionServer
from rdd import DependencyMissing


logger = logging.getLogger(__name__)


class Worker(object):

    def __init__(self, uuid=str(uuid.uuid4()), ip=get_my_ip(), port=get_open_port()):
        self.uuid = uuid
        self.ip = ip
        self.port = port
        self.address = get_my_address(port=self.port)
        self.service = Service(type=WORKER_DISCOVER_TYPE,
                               name='Spark_'+self.uuid, port=self.port, properties=self.get_properties())
        atexit.register(lambda: self.__del__())
        logger.info('Worker '+self.uuid+' is running at '+self.address)
        print('Worker '+self.uuid+' is running at '+self.address)

    def __del__(self):
        self.service.close()

    def get_properties(self):
        return {'uuid': self.uuid, 'address': self.address}


def get_partition_from_job(job):
    try:
        c = zerorpc.Client()
        c.connect(job.address)
        obj_str = c.take(job.uuid)
    except zerorpc.RemoteError, zerorpc.LostRemote:
        return None
    else:
        logger.info('take job:' + job.address)
        return load(obj_str)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, filename='worker.log', filemode='a')
    logger.critical("\n=====Worker Start=====\n")

    # 1. broadcast a `worker` with new generated uuid, {address=ip:port}
    worker = Worker()
    # 2. discover `job`, append to jobs
    job_discover = JobDiscover()
    # 3. discover `partitions`, append to partitions
    partition_discover = PartitionDiscover()
    # 4. start a partitions_server
    partition_server = PartitionServer()
    # 5. start a loop keep trying to get a job from jobs:
    while True:
        print('Fetching job...')
        # 1. connect to job's source, lock it up to prevent other workers to take it
        next_job = job_discover.take_next_job()  # block here
        print('Got job.')
        # 2. get the dumped_partition, unload it
        partition = get_partition_from_job(next_job)
        if partition is None:
            print 'Remote error at getting partition. Skip.'
            continue
        print 'got partition'
        # 3. run the target_partition
        try:
            result = partition.get()
        except DependencyMissing:
            job_discover.suspend_job(next_job)
            print 'Wide dependency missing. Suspend the job.'
            continue
        print 'got result:'+str(result)
        # 4. add result to the partition server
        partition_server.add(partition.uuid, result)
