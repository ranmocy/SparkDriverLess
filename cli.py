import sys
import code
import readline
import rlcompleter

from gevent import fileobject


# Save readline history
# https://docs.python.org/2/tutorial/interactive.html
import atexit
import os
import readline
import rlcompleter

historyPath = os.path.expanduser("~/.pyhistory")

def save_history(historyPath=historyPath):
    import readline
    readline.write_history_file(historyPath)

if os.path.exists(historyPath):
    readline.read_history_file(historyPath)

atexit.register(save_history)
del os, atexit, readline, rlcompleter, save_history, historyPath


# Interactive CLI
class CLI(code.InteractiveConsole):
    def __init__(self, locals=None, filename="<console>", histfile=None):
        self._green_stdin = fileobject.FileObject(sys.stdin)
        self._green_stdout = fileobject.FileObject(sys.stdout)

        code.InteractiveConsole.__init__(self, locals, filename)
        try:
            import readline
        except ImportError:
            pass
        else:
            try:
                import rlcompleter
                readline.set_completer(rlcompleter.Completer(locals).complete)
            except ImportError:
                pass
            readline.parse_and_bind("tab: complete")

        # Preload rdds
        self.runcode(code.compile_command("from rdd import *"))

        self.interact("Welcome to SparkP2P!")

    # def raw_input(self, prompt):
    #     """Override default input function"""
    #     self._green_stdout.write("SparkP2P>>>")
    #     return self._green_stdin.readline()[:-1]
