import code
import atexit
import os
import readline


# Interactive CLI
def load_history(history_file):
    # Save readline history
    # https://docs.python.org/2/tutorial/interactive.html
    if os.path.exists(history_file):
        readline.read_history_file(history_file)

    def save_history():
        import readline

        readline.write_history_file(history_file)

    atexit.register(save_history)


class CLI(code.InteractiveConsole):
    def __init__(self, local=None, filename="<console>", history_file=os.path.expanduser("~/.pyhistory")):
        load_history(history_file)

        # Pre-defined
        local['run'] = self.run
        local['run_file'] = self.run_file

        code.InteractiveConsole.__init__(self, local, filename)
        try:
            import readline
        except ImportError:
            pass
        else:
            try:
                import rlcompleter

                readline.set_completer(rlcompleter.Completer(local).complete)
            except ImportError:
                pass
            readline.parse_and_bind("tab: complete")

        self.interact("Welcome to SparkP2P!")

    def run(self, code_string):
        return_value = None
        for line in code_string.split("\n"):
            return_value = self.runcode(code.compile_command(line))
        return return_value

    def run_file(self, filename):
        with open(filename, 'r') as f:
            return self.run(f.read().rstrip())
