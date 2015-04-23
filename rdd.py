
class RDD(object):

    def __init__(self):
        pass

    def collect(self):
        elements = []
        while True:
            element = self.get()
            if element == None:
                break
            elements.append(element)
        return elements

    def count(self):
        return len(self.collect())

class TextFile(RDD):

    def __init__(self, filename):
        self.filename = filename
        self.lines = None
        self.index = 0

    def get(self):
        if not self.lines:
            f = open(self.filename)
            self.lines = f.readlines()
            f.close()
    
        if self.index == len(self.lines):
            return None
        else:
            line = self.lines[self.index]
            self.index += 1
            return line

class Map(RDD):

    def __init__(self, parent, func):
        self.parent = parent
        self.func = func

    def get(self):
        element = self.parent.get()
        if element == None:
            return None
        else:
            element_new = self.func(element)
            return element_new

class Filter(RDD):
    
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func

    def get(self):
        while True:
            element = self.parent.get()
            if element == None:
                return None
            else:
                if self.func(element):
                    return element


if __name__ == '__main__':

    r = TextFile('myfile')
    m = Map(r, lambda s: s.split())
    f = Filter(m, lambda a: int(a[1]) > 2)
    print f.collect()







