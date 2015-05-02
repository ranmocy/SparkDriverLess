import os
import StringIO
import cloudpickle


PARTITION_NUM = 2


class Context(object):
    def __init__(self, worker=None, driver=None):
        self.worker = worker
        self.driver = driver
        if self.driver:
            self.workers = driver.workers
            PARTITION_NUM = len(self.workers)

    def textFile(self, filename):
        return TextFile(self, filename)


class Partition(object):
    """`part_id` starts from 0"""
    def __init__(self, rdd, part_id):
        super(Partition, self).__init__()
        self.rdd = rdd
        self.part_id = part_id
        self.data = None

    def get(self):
        if self.data is None:
            self.data = []
            for element in self.rdd.get(self.part_id):
                self.data.append(element)
        return self.data


class RDD(object):
    def __init__(self, context):
        self.context = context
        self.partitions = [Partition(self, i) for i in range(PARTITION_NUM)]

    def dump(self):
        output = StringIO.StringIO()
        cloudpickle.CloudPickler(output).dump(self)
        return output.getvalue()

    def map(self, *arg):
        return Map(self.context, self, *arg)
    def filter(self, *arg):
        return Filter(self.context, self, *arg)
    # def reduce(self, *arg):
    #   return Reduce(self.context, self, *arg)

    def collect(self):
        elements = []
        for partition in self.partitions:
            for element in partition.get():
                elements.append(element)
        return elements

    def count(self):
        return len(self.collect())

    def get(self, part_id):
        raise Exception("Need to be implemented in subclass.")


class TextFile(RDD):
    def __init__(self, context, filename):
        super(TextFile, self).__init__(context)
        self.filename = filename

    def get(self, part_id):
        size = os.path.getsize(self.filename)
        length = size / PARTITION_NUM
        offset = length * part_id
        if part_id is PARTITION_NUM - 1:  # last one
            length = size - offset

        with open(self.filename) as handler:
            handler.seek(offset)
            if part_id is not 0:  # unless it's first one, ignore the first line
                handler.readline()
            while length >= 0:  # read lines until to the end
                line = handler.readline()
                if len(line) is 0:  # reach the end of file
                    break
                length -= len(line)
                yield line


class Map(RDD):
    def __init__(self, context, parent, func):
        super(Map, self).__init__(context)
        self.parent = parent
        self.func = func

    def get(self, part_id):
        for element in self.parent.get(part_id):
            yield self.func(element)


class Filter(RDD):
    def __init__(self, context, parent, func):
        super(Filter, self).__init__(context)
        self.parent = parent
        self.func = func

    def get(self, part_id):
        for element in self.parent.get(part_id):
            if self.func(element):
                yield element


if __name__ == '__main__':
    context = Context()
    f = context.textFile('myfile').map(lambda s: s.split()).filter(lambda a: int(a[1]) > 2)
    print f.collect()
