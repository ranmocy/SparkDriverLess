#!/usr/bin/env python

import logging
import itertools

logging.basicConfig(level=logging.DEBUG, filename='client.log', filemode='a')
logger = logging.getLogger(__name__)
logger.critical("\n=====Client Start=====\n")

import os
import uuid

import gevent

from helper import lazy_property, lazy, singleton, dump, DependencyMissing
from partition_caster import PartitionDiscover
from worker import WorkerDiscover
from job_caster import JobServer


class Partition(object):
    def __init__(self, part_id=None, func=None):
        print self.__class__.__name__
        self.part_id = part_id
        self.uuid = str(hash(dump(func))) + ':' + str(part_id)
        self.func = func
        self.evaluated = False
        self.parent_list = []

    @lazy
    def get(self):
        result = self.func(self)
        self.evaluated = True
        return result

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
        self.parent = parent
        self.context = Context()
        self.wide_dependency = False
        self.get = None

    def map(self, *args):
        return Map(self, *args)

    def flat_map(self, *args):
        return FlatMap(self, *args)

    def filter(self, *args):
        return Filter(self, *args)

    def group_by(self, *args):
        return GroupBy(self, *args)

    def reduce(self, *args):
        return Reduce(self, *args)

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
    #     2. if wide_dependency, parent_list is all parent_partitions
    @lazy_property
    def partitions(self):
        if self.parent is None:
            return [Partition(part_id=i, func=self.get) for i in range(self.partition_num)]

        partitions = []
        parent_partitions = self.parent.partitions
        if self.partition_num != len(parent_partitions):
            raise Exception(
                "partitions length mismatched with parent!" + str(len(partitions)) + ',' + str(self.partition_num))
        for i in range(self.partition_num):
            p = Partition(part_id=i, func=self.get)
            if self.wide_dependency:
                p.parent_list = parent_partitions
            else:
                p.parent_list = [parent_partitions[i]]
            partitions.append(p)
        return partitions

    # - when action:
    @lazy
    def collect(self):

        # 1. create partitions from rdds (partition_num = len(workers))
        # 2. for every target_partition in partitions, find in partition_discover:
        #     - if exists, fetch result from corresponding worker
        partition_discover = self.context.partition_discover
        results = [partition_discover.get_partition(partition.uuid) for partition in self.partitions]
        print 'collect', results

        # add to job server if missing
        job_server = self.context.job_server
        for i in range(self.partition_num):
            # - if doesn't exist, or previous try failed
            if results[i] is None:
                # - broadcast a `job` with partition uuid
                job_server.add(self.partitions[i])

        # 3. keep discovering rdds until found the target_rdd
        while True:
            missing_index = [None if result is not None else i for i, result in enumerate(results)]
            missing_index = filter(lambda m: m is not None, missing_index)
            if len(missing_index) is 0:
                break
            print 'keep discovering', missing_index

            for i in missing_index:
                partition = self.partitions[i]
                # try to fetch again
                results[i] = partition_discover.get_partition(partition.uuid)
                # if success this time
                if results[i] is not None:
                    # 4. stop broadcast the `job`
                    print 'stop '+str(i)
                    job_server.remove(partition)

            gevent.sleep(0.1)

        # 5. retrieve result of the rdd
        result = []
        for element in results:
            result += element
        return result


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
            assert len(partition.parent_list) is 1
            return map(func, partition.parent_list[0].get())
        self.get = get


class FlatMap(RDD):
    def __init__(self, parent, func):
        super(FlatMap, self).__init__(parent)

        def get(partition):
            assert len(partition.parent_list) is 1
            l = map(func, partition.parent_list[0].get())
            return [x for item in l for x in item]
        self.get = get


class Filter(RDD):
    def __init__(self, parent, func):
        super(Filter, self).__init__(parent)

        def get(partition):
            assert len(partition.parent_list) is 1
            return filter(func, partition.parent_list[0].get())
        self.get = get


class Repartition(RDD):
    def __init__(self, parent, key_func):
        super(Repartition, self).__init__(parent)
        self.wide_dependency = True

        def get(partition):
            not_evaluated = filter(lambda p: not p.evaluated, partition.parent_list)
            if not_evaluated:
                raise DependencyMissing

            partition_num = len(partition.parent_list)
            filter_func = lambda item: (hash(key_func(item)) % partition_num) is partition.part_id

            new_partition = []
            for parent_partition in partition.parent_list:
                new_partition += filter(filter_func, parent_partition.get())

            return new_partition
        self.get = get


class GroupBy(RDD):
    def __init__(self, parent, func):
        repartition = Repartition(parent, func)
        super(GroupBy, self).__init__(repartition)

        def get(partition):
            assert len(partition.parent_list) is 1
            parent_result = partition.parent_list[0].get()

            h = {}
            for result in parent_result:
                key = func(result)
                if key not in h:
                    h[key] = []
                h[key].append(result)

            result = []
            for key, values in enumerate(h):
                print list(values)
                result.append((key, list(values)))
            return result
        self.get = get


class Reduce(RDD):
    def __init__(self, parent, func):
        super(Reduce, self).__init__(parent)

        def get(partition):
            pass
        self.get = get


class ReduceByKey(RDD):
    def __init__(self, parent, func):
        repartition = Repartition(parent, lambda item: item)
        super(ReduceByKey, self).__init__()


if __name__ == '__main__':
    context = Context()

    f = context\
        .text_file('myfile')\
        .flat_map(lambda line: line.split())\
        .filter(lambda item: item.isalpha())\
        .map(lambda item: (item, 1))\
        .group_by(lambda item: item[0])
    print f.collect()
