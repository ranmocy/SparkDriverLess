import StringIO
from collections import deque
import pickle

import gevent
import zerorpc

from colors import success, error
from rdd import *


class WorkerClient():
    def __init__(self, address):
        self.address = address
        self.connection = zerorpc.Client()
        self.connection.connect(address)
        # print success(self.connection.hello())

    def __del__(self):
        self.connection.close()
        super(WorkerClient, self).__del__()

    def run(self, *args):
        return self.connection.run(*args)

    # Method missing, for delegate method call to RPC
    def __getattr__(self, name):
        def _missing(*args, **kwargs):
            return getattr(self.connection, name)(*args, **kwargs)
        return _missing


class Driver(object):
    def __init__(self):
        self.workers = deque()
        self.thread = gevent.spawn(self.run)

    def join(self):
        self.thread.join()

    def do_job(self, worker):
        # f = TextFile('myfile').map(lambda s: s.split()).filter(lambda a: int(a[1]) > 2)
        # string = f.dump()
        # print worker.run(string)
        # print success(worker.hello())
        pass

    def add_worker(self, address):
        worker = WorkerClient(address)
        self.workers.append(worker)
        print success('Add worker:'+address)

    def remove_worker(self, address):
        for worker in list(self.workers):
            if worker.address == address:
                self.workers.remove(worker)

    def get_next_worker(self):
        try:
            return self.workers.popleft()
        except IndexError:
            return None

    def run(self):
        while True:
            worker = None
            try:
                while worker is None:
                    worker = self.get_next_worker()
                    if worker is None:
                        print("No worker")
                        gevent.sleep(0.5)
                print 'Got worker.'
                print success(worker.hello())
                self.do_job(worker)
                print 'Finished'
            except Exception, e:
                print error(e)
            finally:
                self.workers.append(worker)
                gevent.sleep(2)


if __name__ == '__main__':
    Driver().join()
