import StringIO
import cloudpickle


class Context(object):
    def __init__(self, worker=None, driver=None):
        self.worker = worker
        self.driver = driver
        if not self.driver:
            self.workers = driver.workers

    def textFile(self, filename):
        return TextFile(self, filename)


class RDD(object):
    def __init__(self, context):
        self.context = context

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
        while True:
            element = self.get()
            if element is None:
                break
            elements.append(element)
        return elements

    def count(self):
        return len(self.collect())

    def get(self):
        raise Exception("Need to be implemented in subclass.")


class TextFile(RDD):
    def __init__(self, context, filename):
        super(TextFile, self).__init__(context)
        self.filename = filename
        self.lines = None
        self.index = 0

    def get(self):
        if not self.lines:
            with open(self.filename) as handler:
                self.lines = handler.readlines()

        if self.index == len(self.lines):
            return None
        else:
            line = self.lines[self.index]
            self.index += 1
            return line


class Map(RDD):
    def __init__(self, context, parent, func):
        super(Map, self).__init__(context)
        self.parent = parent
        self.func = func

    def get(self):
        element = self.parent.get()
        if element is None:
            return None
        else:
            element_new = self.func(element)
            return element_new


class Filter(RDD):
    def __init__(self, context, parent, func):
        super(Filter, self).__init__(context)
        self.parent = parent
        self.func = func

    def get(self):
        while True:
            element = self.parent.get()
            if element is None:
                return None
            else:
                if self.func(element):
                    return element


if __name__ == '__main__':
    context = Context()
    f = context.textFile('myfile').map(lambda s: s.split()).filter(lambda a: int(a[1]) > 2)
    print f.collect()
