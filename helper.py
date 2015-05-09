import StringIO
import logging
import socket
import sys
import signal
import subprocess
import pickle

import cloudpickle


logger = logging.getLogger(__name__)


class DependencyMissing(Exception):
    pass


def get_open_port():
    for i in range(10):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("", 0))
            s.listen(1)
            port = s.getsockname()[1]
            s.close()
            return port
        except Exception:
            continue
    raise Exception('No available port')


def get_my_ip():
    return subprocess.Popen(["hostname", "-I"], stdout=subprocess.PIPE).communicate()[0].strip()


def get_zerorpc_address(ip=get_my_ip(), port=get_open_port()):
    return 'tcp://'+ip+':'+str(port)


def bind_signal_handler(obj):
    def signal_handler(sig, frame):
        try:
            logger.warning("Catch " + str(sig))
            obj.__del__()
            sys.exit()
        except Exception, e:
            logger.error(str(e))
            sys.exit()
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


# Pickle
def dump(obj):
    return cloudpickle.dumps(obj)


def load(obj_str):
    return pickle.loads(obj_str)


# Decorators
def singleton(cls):
    obj = cls()
    # Always return the same object
    cls.__new__ = staticmethod(lambda cls: obj)
    # Disable __init__
    try:
        del cls.__init__
    except AttributeError:
        pass
    return cls


def lazy_property(fn):
    attr_name = '_lazy_' + fn.__name__
    @property
    def _lazy_prop(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_prop


def lazy(fn):
    def _lazy_func(self, *args):
        attr_name = '_lazy_func_' + fn.__name__ + str(args)
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self, *args))
        return getattr(self, attr_name)
    return _lazy_func
