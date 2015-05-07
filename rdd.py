import StringIO
import logging
import os
import uuid

import gevent
import zerorpc
import cloudpickle

from helper import lazy_property, lazy, singleton, dump
from partition_discover import PartitionDiscover
from worker_discover import WorkerDiscover
from job_server import JobServer


logger = logging.getLogger(__name__)


class Partition(object):
    def __init__(self, uuid=None, part_id=None, func=None):
        self.rdd_uuid = uuid
        self.part_id = part_id
        self.uuid = str(uuid) + ':' + str(part_id)
        self.func = func
        self.parent_list = []

    @lazy
    def get(self):
        return self.func(self)

    def dump(self):
        return dump(self)


@singleton
class Context(object):
    def __init__(self):
        # 1. discover `worker`, append to workers
        self.worker_discover = WorkerDiscover()
        # 2. discover `partitions`, append to partitions
        self.partition_discover = PartitionDiscover()
        # 3. start job_server
        self.job_server = JobServer()
        # 4. start console for the user
        pass

    def text_file(self, filename):
        return TextFile(filename)


class RDD(object):
    def __init__(self, parent):
        """[parent,func] or [context], one and only one."""
        self.uuid = str(uuid.uuid1())
        self.parent = parent
        self.context = Context()
        self.get = None

    def map(self, *args):
        return Map(self, *args)

    def filter(self, *args):
        return Filter(self, *args)

    # def reduce(self, *args):
    # return Reduce(self, *args)

    @lazy_property
    def partition_num(self):
        if self.parent:
            return self.parent.partition_num
        else:
            num = self.context.worker_discover.size()
            return num if num >= 2 else 2

    # GetPartition
    # - when transition:
    #     1. create rdd lineage
    @lazy_property
    def partitions(self):
        if self.parent is None:
            return [Partition(uuid=self.uuid, part_id=i, func=self.get) for i in range(self.partition_num)]

        partitions = []
        parent_partitions = self.parent.partitions
        if self.partition_num != len(parent_partitions):
            print self
            raise Exception(
                "partitions length mismatched with parent!" + str(len(partitions)) + ',' + str(self.partition_num))
        for i in range(self.partition_num):
            p = Partition(uuid=self.uuid, part_id=i, func=self.get)
            p.parent_list = [parent_partitions[i]]
            partitions.append(p)
        return partitions

    # - when action:
    @lazy
    def collect(self):
        print 'collect',
        elements = []
        # 1. create partitions from rdds (partition_num = len(workers))
        # 2. for every target_partition in partitions, find in partition_discover:
        for partition in self.partitions:
            result = self.context.partition_discover.get_partition(partition.uuid)
            # - if exists, fetch result from corresponding worker
            elements.append(result)
            if result is None:
                # - if doesn't exist, or previous try failed
                #     1. append to partition_server
                #     2. broadcast a `job` with partition uuid
                self.context.job_server.add(partition)
        print elements

        # 3. keep discovering rdds until found the target_rdd
        while True:
            print 'keep discovering',
            all_done = True
            for i in range(self.partition_num):
                if elements[i] is None:
                    print ' ' + str(i),
                    # try to fetch again
                    elements[i] = self.context.partition_discover.get_partition(self.partitions[i].uuid)
                # if failed
                if elements[i] is None:
                    all_done = False
                else:
                    # 4. stop broadcast the `job`
                    self.context.job_server.remove(self.partitions[i])
            print ""
            if all_done:
                break
            gevent.sleep(1)

        # 5. retrieve result of the rdd
        result = []
        for element in elements:
            result += element
        return result

    # Run
    # - if narrow_dependent:
    #     - do it right away

    # - if wide_dependent:
    def get_wide(self, partition):
        # - try search dep_partitions in rdds
        if self.uuid in self.context.rdds:
            # - if exists, fetch result from corresponding worker
            for address in self.context.rdds[self.uuid]:
                c = zerorpc.Client()
                c.connect(address)
                try:
                    return c.fetch_rdd(self.uuid)
                except zerorpc.RemoteError, zerorpc.LostRemote:
                    continue
                    # - if doesn't exist, or previous try failed
                    # 1. for every partition of dep_rdd:

                    #     1. append to partitions_server
                    #     2. broadcast a `job` with partition uuid
                    # 2. append current job back to jobs
                    # 3. DO NOT sleep(NETWORK_LATENCY * 2). it's better to it locally to avoid network transfer
                    # 3. continue to next job

            pass


class TextFile(RDD):
    def __init__(self, filename):
        super(TextFile, self).__init__(None)
        self.filename = filename
        partition_num = self.partition_num

        def get(partition):
            part_id = partition.part_id
            size = os.path.getsize(filename)
            length = size / partition_num
            offset = length * part_id
            if part_id is partition_num - 1:  # last one
                length = size - offset

            lines = []
            with open(filename) as handler:
                handler.seek(offset)
                if part_id is not 0:  # unless it's first one, ignore the first line
                    handler.readline()
                while length >= 0:  # read lines until to the end
                    line = handler.readline()
                    if len(line) is 0:  # reach the end of file
                        break
                    length -= len(line)
                    lines.append(line)
            return lines
        self.get = get


class Map(RDD):
    def __init__(self, parent, func):
        super(Map, self).__init__(parent)

        def get(partition):
            assert len(partition.parent_list) == 1
            return map(func, partition.parent_list[0].get())
        self.get = get


class Filter(RDD):
    def __init__(self, parent, func):
        super(Filter, self).__init__(parent)

        def get(partition):
            assert len(partition.parent_list) == 1
            return filter(func, partition.parent_list[0].get())
        self.get = get


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, filename='client.log', filemode='a')
    logger.critical("\n=====Client Start=====\n")

    context = Context()
    f = context.text_file('myfile').map(lambda s: s.split()).filter(lambda a: int(a[1]) > 2)
    print f.collect()
