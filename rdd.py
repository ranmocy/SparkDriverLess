import StringIO
import cloudpickle


class RDD(object):
    def __init__(self):
        pass

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
    def __init__(self, filename):
        super(TextFile, self).__init__()
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
    def __init__(self, parent, func):
        super(Map, self).__init__()
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
    def __init__(self, parent, func):
        super(Filter, self).__init__()
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
    r = TextFile('myfile')
    m = Map(r, lambda s: s.split())
    f = Filter(m, lambda a: int(a[1]) > 2)
    print f.collect()
