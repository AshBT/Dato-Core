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
from string import lower
import graphlab.connect.server as server
import os
import random
import time
import tempfile
import shutil
import math
from pandas.util.testing import assert_frame_equal

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
