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
"""
Provides utility context managers related
to executing cython functions
"""

import os


class debug_trace(object):
    """
    A context manager that suppress the cython
    stack trace in release build.
    """

    def __init__(self):
        self.show_cython_trace = 'GRAPHLAB_UNITY' in os.environ

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.show_cython_trace and exc_type:
            raise exc_type(exc_value)
