import logging
import os
import StringIO
import uuid

import zerorpc
import cloudpickle

from helper import lazy_property, lazy, singleton
from partition_discover import PartitionDiscover
from worker_discover import WorkerDiscover
from job_server import JobServer


logger = logging.getLogger(__name__)


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


class Partition(object):
    """`part_id` starts from 0"""
    def __init__(self, rdd, part_id):
        super(Partition, self).__init__()
        self.rdd = rdd
        self.part_id = part_id
        self.parent_list = []

    @lazy_property
    def uuid(self):
        return self.rdd.uuid + ':' + self.part_id

    @lazy
    def get(self):
        return self.rdd.get(self)

    def add_to_partition_server(self):
        self.rdd.context.partition_server.add(self.uuid, self.get())


class RDD(object):
    def __init__(self, parent):
        """[parent,func] or [context], one and only one."""
        self.uuid = str(uuid.uuid1())
        self.parent = parent
        self.context = Context()

    def dump(self):
        output = StringIO.StringIO()
        cloudpickle.CloudPickler(output).dump(self)
        return output.getvalue()

    def map(self, *args):
        return Map(self, *args)

    def filter(self, *args):
        return Filter(self, *args)

    # def reduce(self, *args):
    #   return Reduce(self, *args)

    @lazy_property
    def partition_num(self):
        if self.parent:
            return self.parent.partition_num
        else:
            return len(self.context.worker_discover.workers)

    # GetPartition
    # - when transition:
    #     1. create rdd lineage
    @lazy_property
    def partitions(self):
        partitions = []
        parent_partitions = self.parent.partitions
        if len(partitions) != len(parent_partitions):
            raise Exception("partitions length mismatched with parent!")
        for i in range(self.partition_num):
            p = Partition(self, i)
            p.parent_list = [parent_partitions[i]]
            partitions.append(p)
        return partitions

    # - when action:
    @lazy
    def collect(self):
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
        # 3. keep discovering rdds until found the target_rdd
        while True:
            all_done = True
            for i in range(self.partition_num):
                if elements[i] is not None:
                    continue
                partition = self.partitions[i]
                # try to fetch again
                elements[i] = self.context.partition_discover.get_partition(partition.uuid)
                if elements[i] is None:
                    all_done = False
                    break
                else:
                    # 4. stop broadcast the `job`
                    self.context.job_server.remove(partition)
            if all_done:
                break
        # 5. retrieve result of the rdd
        result = []
        for element in elements:
            result += element
        return result

    # Run
    # - if narrow_dependent:
    #     - do it right away
    @lazy
    def get(self, partition):
        raise Exception("Need to be implemented in subclass.")

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

    @lazy_property
    def partitions(self):
        return [Partition(self, i) for i in range(self.partition_num)]

    @lazy
    def get(self, partition):
        part_id = partition.part_id
        size = os.path.getsize(self.filename)
        length = size / self.partition_num
        offset = length * part_id
        if part_id is self.partition_num - 1:  # last one
            length = size - offset

        lines = []
        with open(self.filename) as handler:
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


class Map(RDD):
    def __init__(self, parent, func):
        super(Map, self).__init__(parent)
        self.func = func

    @lazy
    def get(self, partition):
        result = []
        for element in self.parent.get():
            result.append(self.func(element))
        return result


class Filter(RDD):
    def __init__(self, parent, func):
        super(Filter, self).__init__(parent)
        self.func = func

    @lazy
    def get(self, partition):
        result = []
        for element in self.parent.get():
            if self.func(element):
                result.append(element)
        return result


if __name__ == '__main__':
    context = Context()
    f = context.text_file('myfile').map(lambda s: s.split()).filter(lambda a: int(a[1]) > 2)
    print f.collect()
