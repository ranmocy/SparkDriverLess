import atexit
from collections import deque
import logging
import threading

import gevent
import zerorpc

from broadcast import Service, Discover, Broadcaster
from helper import get_my_ip, get_open_port, get_my_address, load


__author__ = 'ranmocy'
_JOB_CASTER_TYPE = '_spark.job.'
logger = logging.getLogger(__name__)


class JobServerHandler(object):
    def __init__(self, services, address=get_my_address(get_open_port())):
        self.services = services
        self.address = address
        self.server = zerorpc.Server(self)
        self.server.bind(self.address)
        self.thread = gevent.spawn(self.server.run)
        logger.info("Job server started at " + self.address)
        self.lock = threading.Lock()

    def __del__(self):
        self.thread.kill()

    def take(self, uuid):
        try:
            self.lock.acquire()
            if uuid not in self.services:  # job is finished
                print 'finished job', uuid
                return None
            service = self.services[uuid]
            if not service.is_active():  # job is taken
                print 'taken job'
                return None

            # De-activate to avoid multiple worker doing same job
            service.deactivate()
            logger.debug("Deactivated job:" + service.name)

            # - if it's taken, set a timer.
            #     - If timeout and no result, broadcast again since that worker is dead, or too slow.
            services = self.services

            def reactivate():
                if service.partition.uuid in services:
                    # Still not finished
                    service.activate()
                    logger.debug("Reactivated job:" + service.name)
            gevent.spawn_later(10, reactivate)

            logger.debug("Return job:" + service.name)
            return service.partition.dump()
        finally:
            self.lock.release()


class JobServer(Broadcaster):
    def __init__(self):
        super(JobServer, self).__init__(name='Spark.JobServer')
        self.ip = get_my_ip()
        self.port = get_open_port()
        self.address = get_my_address(port=self.port)
        self.jobs = {}  # uuid => service
        self.handler = JobServerHandler(self.jobs, address=self.address)
        atexit.register(lambda: self.__del__())

    def __del__(self):
        super(JobServer, self).__del__()
        self.handler.__del__()

    def add(self, partition):
        uuid = partition.uuid
        if uuid in self.jobs:
            logger.warning('duplicated job service:' + uuid)
            return
        service = Service(name=uuid, type=_JOB_CASTER_TYPE, location=self.ip, port=self.port)
        service.partition = partition  # attach additional information for handler

        self.job[uuid] = service
        super(JobServer, self).add(service)

    def remove(self, partition):
        uuid = partition.uuid
        if uuid in self.jobs:
            service = self.jobs[uuid]
            del self.jobs[uuid]
            super(JobServer, self).remove(service)


class JobDiscover(Discover):
    def __init__(self):
        queue = deque()
        self.queue = queue
        discover = self

        def found_func(seeker, result):
            for uuid in discover.results:
                if result.uuid == uuid:
                    return
            queue.append(result)
            logger.info("Found "+result.type+":"+result.sname+" at "+result.address)

        super(JobDiscover, self).__init__(type=_JOB_CASTER_TYPE, found_func=found_func)

    def take_next_job(self):
        while True:
            try:
                result = self.queue.pop()
                if result.uuid not in self.results:
                    # outdated result
                    gevent.sleep(0.1)
                    continue
            except IndexError:
                gevent.sleep(0.1)
                continue
            else:
                return result

    def suspend_job(self, result):
        self.queue.append(result)