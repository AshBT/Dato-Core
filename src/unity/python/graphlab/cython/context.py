"""
Provides utility context managers related
to executing cython functions
"""

'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''

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
