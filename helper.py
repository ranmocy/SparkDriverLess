import socket
import sys
import signal
import subprocess

from colors import warn


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


def get_my_address(port=get_open_port()):
    return 'tcp://'+get_my_ip()+':'+str(port)


def bind_signal_handler(obj):
    def signal_handler(sig, frame):
        try:
            print warn("Catch " + str(sig))
            obj.__del__()
            sys.exit()
        except Exception, e:
            print e
            sys.exit()
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
