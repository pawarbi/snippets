!pip install pygments --q

import inspect
from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import TerminalFormatter

def show_source(function):
    code = inspect.getsource(function)

    # Now format the source code for better readability
    formatted_code = highlight(code, PythonLexer(), TerminalFormatter())

    return print(formatted_code)
