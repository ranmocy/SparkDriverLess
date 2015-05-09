#!/usr/bin/env python
import logging
logging.basicConfig(level=logging.DEBUG, filename='worker.log', filemode='a')
logger = logging.getLogger(__name__)
logger.critical("\n=====Worker Start=====\n")

import uuid

import zerorpc

from helper import get_my_ip, get_zerorpc_address, get_open_port, load, DependencyMissing
from broadcast import Service, Discover, Broadcaster
from job_caster import JobDiscover, JobServer
from partition_caster import PartitionServer, PartitionDiscover


_WORKER_CASTER_TYPE = '_spark.worker.'


class WorkerServer(Broadcaster):

    def __init__(self):
        super(WorkerServer, self).__init__('Spark.WorkerServer')
        self.uuid = str(uuid.uuid4())
        self.ip = get_my_ip()
        self.port = get_open_port()
        self.address = get_zerorpc_address(port=self.port)
        self.service = Service(type=_WORKER_CASTER_TYPE, name=self.uuid, location=self.ip, port=self.port)
        self.add(self.service)


class WorkerDiscover(Discover):
    def __init__(self):
        super(WorkerDiscover, self).__init__(type=_WORKER_CASTER_TYPE)

    def size(self):
        return len(self.results)


def get_partition_from_job(job):
    try:
        client = zerorpc.Client()
        client.connect(job.address)
        obj_str = client.take(job.uuid)
    except zerorpc.RemoteError, zerorpc.LostRemote:
        return None
    else:
        logger.info('take job:' + job.address)
        return load(obj_str)


if __name__ == '__main__':
    # 1. broadcast a `worker` with new generated uuid, {address=ip:port}
    worker = WorkerServer()
    # 2. discover `job`, append to jobs
    job_discover = JobDiscover()
    # 3. start a job server
    job_server = JobServer()
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
            # - if narrow_dependent:
            #   - do it right away
            result = partition.get()
        except DependencyMissing:
            # - if wide_dependent:
            #   - try search dep_partitions in rdds
            #     - if exists, fetch result from corresponding worker
            not_exists_in_discover = lambda p: partition_discover.get_partition(p.uuid) is None
            missing_partitions = filter(not_exists_in_discover, partition.parent_partitions)
            assert len(missing_partitions) > 0  # otherwise there won't be exception

            # - if doesn't exist, or previous try failed
            # 1. for every partition of dep_rdd:
            for missing in missing_partitions:
                # 1. append to job server
                # 2. broadcast a `job` with partition uuid
                job_server.add(missing)
            # 2. append current job back to jobs
            job_discover.suspend_job(next_job)
            print 'Wide dependency missing. Suspend the job.'
            # 3. DO NOT sleep(NETWORK_LATENCY * 2). it's better to it locally to avoid network transfer
            # 3. continue to next job
            continue
        print 'got result:'+str(result)
        # 4. add result to the partition server
        partition_server.add(uuid=partition.uuid, result=result)
