'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''
from string import lower
import graphlab.connect.server as server
import os
import random
import time
import tempfile
import shutil
import math
import string
import numpy as np
from pandas.util.testing import assert_frame_equal
import graphlab as gl

class SFrameComparer():
    """
    Helper class for comparing sframe and sarrays

    Adapted from test_sframe.py
    """
    def _assert_sframe_equal(self, sf1, sf2):
        assert sf1.num_rows() == sf2.num_rows()
        assert sf1.num_cols() == sf2.num_cols()
        assert set(sf1.column_names()) == set(sf2.column_names())
        assert_frame_equal(sf1.to_dataframe(), sf2.to_dataframe())

    def _assert_sarray_equal(self, sa1, sa2):
        l1 = list(sa1)
        l2 = list(sa2)
        assert len(l1) == len(l2)
        for i in range(len(l1)):
            v1 = l1[i]
            v2 = l2[i]
            if v1 == None:
                assert v2 == None
            else:
                if type(v1) == dict:
                    assert len(v1) == len(v2)
                    for key in v1:
                        assert v1.has_key(key)
                        assert v1[key] == v2[key]

                elif (hasattr(v1, "__iter__")):
                    assert len(v1) == len(v2)
                    for j in range(len(v1)):
                        t1 = v1[j]; t2 = v2[j]
                        if (type(t1) == float):
                            if (math.isnan(t1)):
                                assert math.isnan(t2)
                            else:
                                assert t1 == t2
                        else:
                            assert t1 == t2
                else:
                    assert v1 == v2


class SubstringMatcher():
    """
    Helper class for testing substring matching

    Code adapted from http://www.michaelpollmeier.com/python-mock-how-to-assert-a-substring-of-logger-output/
    """
    def __init__(self, containing):
        self.containing = lower(containing)

    def __eq__(self, other):
        return lower(other).find(self.containing) > -1

    def __unicode__(self):
        return 'a string containing "%s"' % self.containing

    def __str__(self):
        return unicode(self).encode('utf-8')
    __repr__ = __unicode__

class TempDirectory():
    name = None
    def __init__(self):
        self.name = tempfile.mkdtemp()
    def __enter__(self):
        return self.name
    def __exit__(self, type, value, traceback):
        if self.name != None:
            shutil.rmtree(self.name)

def create_server(server_addr, auth_token=None, public_key='', secret_key=''):
    """
    Creates a server process, which listens on the server_addr, and uses
    auth_token. This function can be used for testing scenrios for remote launches, e.g.:

    >>> server = create_server('tcp://127.0.0.1:10000')
    >>> graphlab.launch('ipc://127.0.0.1:10000')
    >>> assert graphlab.connect.main.is_connected()

    Returns the LocalServer object which manages the server process.
    """
    server_bin = os.getenv("GRAPHLAB_UNITY")
    product_key = os.getenv("GRAPHLAB_PRODUCT_KEY")
    ts = str(int(time.time()))
    unity_log = "/tmp/test_connect_%s.log" % ts
    return server.LocalServer(server_addr, server_bin, unity_log, product_key, auth_token=auth_token,
                              public_key=public_key, secret_key=secret_key)


def start_test_tcp_server(auth_token=None):
    MAX_NUM_PORT_ATTEMPTS = 10
    num_attempted_ports = 0
    server = None

    while(num_attempted_ports < MAX_NUM_PORT_ATTEMPTS):
        port = random.randint(8000, 65535)
        tcp_addr = 'tcp://127.0.0.1:%d' % port
        server = create_server(tcp_addr, auth_token)
        num_attempted_ports += 1

        try:
            server.start()
        except:
            # Occasionally we pick a port that's already in use.
            server.try_stop()
        else:
            break  # Success, port was not in use.

    assert(num_attempted_ports < MAX_NUM_PORT_ATTEMPTS)
    return server


def uniform_string_column(n, word_length, alphabet_size, missingness=0.):
    """
    Return an SArray of strings constructed uniformly randomly from the first
    'num_letters' of the lower case alphabet.

    Parameters
    ----------
    n : int
        Number of entries in the output SArray.

    word_length : int
        Number of characters in each string.

    alphabet_size : int
        Number of characters in the alphabet.

    missingness : float, optional
        Probability that a given entry in the output is missing.

    Returns
    -------
    out : SArray
        One string "word" in each entry of the output SArray.
    """
    result = []
    letters = string.ascii_letters[:alphabet_size]

    for i in range(n):
        missing_flag = random.random()
        
        if missing_flag < missingness:
            result.append(None)
        else:
            word = []
            for j in range(word_length):
                word.append(random.choice(letters))
            result.append(''.join(word))

    return gl.SArray(result)


def uniform_numeric_column(n, col_type=float, range=(0, 1), missingness=0.):
    """
    Return an SArray of uniformly random numeric values.

    Parameters
    ----------
    n : int
        Number of entries in the output SArray.

    col_type : type, optional
        Type of the output SArray. Default is floats.

    range : tuple[int, int], optional
        Minimum and maximum of the uniform distribution from which values are
        chosen.

    missingness : float, optional
        Probability that a given entry in the output is missing.

    Returns
    -------
    out : SArray
    """
    if col_type == int:
        v = np.random.randint(low=range[0], high=range[1], size=n).astype(float)
    else:
        v = np.random.rand(n)
        v = v * (range[1] - range[0]) + range[0]

    idx_na = np.random.rand(n) < missingness
    v[idx_na] = None
    v = np.where(np.isnan(v), None, v)

    return gl.SArray(v, dtype=col_type)
