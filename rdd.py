import logging
import os
import StringIO
import uuid
import zerorpc
import cloudpickle

from helper import lazy_property, lazy


logger = logging.getLogger(__name__)


class Context(object):
    def __init__(self, worker_discover=None, partition_discover=None, partition_server=None):
        self.workers = worker_discover.workers
        self.partitions = partition_discover.partitions
        self.partition_server = partition_server

    def textFile(self, filename):
        return TextFile(filename, context=self)


class Partition(object):
    """`part_id` starts from 0"""
    def __init__(self, rdd, part_id):
        super(Partition, self).__init__()
        self.rdd = rdd
        self.part_id = part_id

    @lazy_property
    def uuid(self):
        return self.rdd.uuid + ':' + self.part_id

    @lazy
    def get(self):
        return self.rdd.get(self.part_id)

    def add_to_partition_server(self):
        self.rdd.context.partition_server.add(self.uuid, self.get())


class RDD(object):
    def __init__(self, context=None):
        """[parent,func] or [context], one and only one."""
        self.uuid = str(uuid.uuid1())
        self.parent = None
        self.func = None
        self._context = context

    def dump(self):
        output = StringIO.StringIO()
        cloudpickle.CloudPickler(output).dump(self)
        return output.getvalue()

    def map(self, *arg):
        return Map(self, *arg)

    def filter(self, *arg):
        return Filter(self, *arg)

    # def reduce(self, *arg):
    #   return Reduce(self, *arg)

    @lazy_property
    def context(self):
        if self._context:
            return self._context
        if self.parent:
            return self.parent.get_context()
        return None

    @lazy_property
    def partition_num(self):
        if self.parent:
            num = self.parent.partition_num
        else:
            num = len(self.context.workers)
        logger.debug('partition_num:' + str(num))
        return num

    # GetPartition
    #     - when transition:
    #         1. create rdd lineage
    #     - when action:
    #         1. create partitions from rdds (partition_num = len(workers))
    #         2. try search target_rdd in rdds
    #             - if exists, fetch result from corresponding worker
    #             - if doesn't exist, or previous try failed
    #                 - for every partition of target_rdd:
    #                     1. append to partitions_server
    #                     2. broadcast a `job` with partition uuid
    #         3. keep discovering rdds until found the target_rdd
    #         4. stop broadcast the `job`
    #         5. retrieve result of the rdd
    @lazy_property
    def partitions(self):
        return [Partition(self, i) for i in range(self.partition_num)]

    def collect(self):
        elements = []
        for partition in self.partitions:
            for element in partition.get():
                elements.append(element)
        return elements

    def count(self):
        return len(self.collect())

    # Run
    # - if narrow_dependent:
    #     - do it right away
    @lazy
    def get(self, part_id):
        raise Exception("Need to be implemented in subclass.")

    # - if wide_dependent:
    def get_wide(self, part_id):
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
    def __init__(self, filename, context=None):
        super(TextFile, self).__init__(context=context)
        self.filename = filename

    @lazy
    def get(self, part_id):
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
        super(Map, self).__init__()
        self.parent = parent
        self.func = func

    @lazy
    def get(self, part_id):
        result = []
        for element in self.parent.get():
            result.append(self.func(element))
        return result


class Filter(RDD):
    def __init__(self, parent, func):
        super(Filter, self).__init__()
        self.parent = parent
        self.func = func

    @lazy
    def get(self, part_id):
        result = []
        for element in self.parent.get():
            if self.func(element):
                result.append(element)
        return result


if __name__ == '__main__':
    context = Context()
    f = context.textFile('myfile').map(lambda s: s.split()).filter(lambda a: int(a[1]) > 2)
    print f.collect()
