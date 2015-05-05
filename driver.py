import logging
import StringIO
from collections import deque
import pickle

import gevent
import zerorpc

from helper import bind_signal_handler
from broadcast import Discover
from rdd import *


logger = logging.getLogger(__name__)


def get_discover_listener(driver):
    class DiscoverListener(object):
        def add_service(self, zeroconf, type, name):
            info = zeroconf.get_service_info(type, name)
            driver.add_worker(info.properties['address'])
            logger.info("Service found: %s" % info.properties['address'])

        def remove_service(self, zeroconf, type, name):
            address = name.replace('SparkP2P_', '').replace(type, '')
            driver.remove_worker(address)
            logger.info("Service removed: %s" % (name,))
    return DiscoverListener()


class WorkerClient(object):
    def __init__(self, address):
        self.address = address
        self._connection = None

    def __del__(self):
        logger.warning('Worker is closing.')
        if self.connection:
            self.connection.close()

    @property
    def connection(self):
        """lazy initalization.
        Can not initalize zerorpc connection in zeroconf thread."""
        if not self._connection:
            self._connection = zerorpc.Client(heartbeat=1)
            self._connection.connect(self.address)
        return self._connection

    # Method missing, for delegate method call to RPC
    def __getattr__(self, name):
        def _missing(*args, **kwargs):
            return getattr(self.connection, name)(*args, **kwargs)
        return _missing


class Driver(object):
    def __init__(self):
        self.workers = deque()
        self.thread = gevent.spawn(self.run)
        self.discover = Discover(listener=get_discover_listener(self))

    def __del__(self):
        self.discover.close()
        logger.info('discover closed')

    def join(self):
        self.thread.join()

    def do_job(self, worker):
        f = TextFile('myfile').map(lambda s: s.split()).filter(lambda a: int(a[1]) > 2)
        string = f.dump()
        logger.debug(worker.run(string))

    def add_worker(self, address):
        worker = WorkerClient(address)
        self.workers.append(worker)
        logger.info('Add worker:'+address)

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
                while not worker:
                    worker = self.get_next_worker()
                    if not worker:
                        logger.debug("No worker")
                        gevent.sleep(0.5)
                logger.info('Got worker.')
                self.do_job(worker)
                logger.info('Finished')
            except zerorpc.exceptions.LostRemote:
                worker = None
            except Exception, e:
                logger.critical(str(e))
            finally:
                if worker:
                    self.workers.append(worker)
                gevent.sleep(1)


if __name__ == '__main__':
    driver = Driver()
    bind_signal_handler(driver)
    driver.join()
