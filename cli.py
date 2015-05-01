import sys
import code
import readline
import rlcompleter

from gevent import fileobject


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
        self.interact("Welcome to SparkP2P!")

    # def raw_input(self, prompt):
    #     """Override default input function"""
    #     self._green_stdout.write("SparkP2P>>>")
    #     return self._green_stdin.readline()[:-1]
