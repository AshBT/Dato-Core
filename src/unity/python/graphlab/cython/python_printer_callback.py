'''
Copyright (C) 2015 Dato, Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''
import sys
try:
    import IPython
    from IPython.core.interactiveshell import InteractiveShell
    have_ipython = True
except ImportError:
    have_ipython = False

def print_callback(val):
    """
    Internal function.
    This function is called via a call back returning from IPC to Cython
    to Python. It tries to perform incremental printing to IPython Notebook and
    when all else fails, just prints locally.
    """
    success = False
    try:
        # for reasons I cannot fathom, regular printing, even directly
        # to io.stdout does not work.
        # I have to intrude rather deep into IPython to make it behave
        if have_ipython:
            if InteractiveShell.initialized():
                IPython.display.publish_display_data('graphlab.callback', {'text/plain':val,'text/html':'<pre>' + val + '</pre>'})
                success = True
    except:
        pass

    if not success:
        print val
        sys.stdout.flush()
