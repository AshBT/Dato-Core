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
import graphlab
import mock
import os
import random
import stat
import tempfile
import unittest
import logging

from graphlab.connect import main as glconnect
from graphlab.connect import server
from graphlab.test.util import SubstringMatcher
from graphlab.test.util import create_server, start_test_tcp_server


class ConnectLocalTests(unittest.TestCase):
    def test_launch(self):
        #default launch
        glconnect.launch()
        self.assertTrue(glconnect.is_connected())
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())

        #launch with server address
        tmpname = tempfile.NamedTemporaryFile().name
        tmpaddr = 'ipc://' + tmpname
        glconnect.launch(tmpaddr)
        self.assertTrue(glconnect.is_connected())
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())
        #check that the ipc file gets deleted
        self.assertFalse(os.path.exists(tmpname))

        #launch address and binary
        graphlab_bin = os.getenv("GRAPHLAB_UNITY")
        glconnect.launch(server_addr=tmpaddr,
                         server_bin=graphlab_bin)
        self.assertTrue(glconnect.is_connected())
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())
        self.assertFalse(os.path.exists(tmpname))

    @mock.patch('graphlab.connect.main.__LOGGER__')
    def test_launch_with_exception(self, mock_logging):
        # Assert warning logged when launching without stopping
        glconnect.launch()
        glconnect.launch()
        self.assertTrue(mock_logging.warning.called_once_with(SubstringMatcher(containing="existing server")))
        self.assertTrue(glconnect.is_connected())
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())

        # launch with bogus server binary (path is not executable)
        with tempfile.NamedTemporaryFile() as f:
            random_server_bin = f.name
            glconnect.launch(server_bin=random_server_bin)
            self.assertTrue(mock_logging.error.called_once_with(SubstringMatcher(containing="Invalid server binary")))
            self.assertFalse(glconnect.is_connected())

        #launch with server address without permission
        tmpaddr = 'ipc:///root/bad_server'
        glconnect.launch(tmpaddr)
        self.assertTrue(mock_logging.warning.called_once_with(SubstringMatcher(containing="communication error")))
        self.assertFalse(glconnect.is_connected())
        glconnect.stop()

        # launch with binary that does not exist
        tmpname = tempfile.NamedTemporaryFile().name
        glconnect.launch(server_bin=tmpname)
        self.assertTrue(mock_logging.error.called_once_with(SubstringMatcher(containing="Invalid server binary")))
        self.assertFalse(glconnect.is_connected())

        # launch with bogus server binary (path is a faked executable)
        with tempfile.NamedTemporaryFile() as f:
            os.chmod(f.name, stat.S_IXUSR)
            random_server_bin = f.name
            glconnect.launch(server_bin=random_server_bin)
            self.assertTrue(mock_logging.error.called_once_with(SubstringMatcher(containing="Invalid server binary")))
            self.assertFalse(glconnect.is_connected())

        # TODO:: launch with bad server binary (takes too long to start or does not connect)


class ConnectRemoteTests(unittest.TestCase):

    def test_launch_to_ipc(self):
        ipc_addr = 'ipc://' + tempfile.NamedTemporaryFile().name
        auth_token = 'graphlab_awesome'
        ipc_server = create_server(ipc_addr, auth_token)
        ipc_server.start()
        #default local launch
        glconnect.launch()
        self.assertTrue(glconnect.is_connected())
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())

        #launch with remote server ipc address
        glconnect.launch(ipc_addr, auth_token=auth_token)
        self.assertTrue(glconnect.is_connected())
        self.assertTrue(isinstance(glconnect.get_server(), server.RemoteServer))
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())

        #launch with remote server addr, and server_bin(ignored)
        glconnect.launch(ipc_addr, os.getenv("GRAPHLAB_UNITY"), auth_token=auth_token)
        self.assertTrue(glconnect.is_connected())
        self.assertTrue(isinstance(glconnect.get_server(), server.RemoteServer))
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())
        ipc_server.stop()

    def test_launch_to_tcp(self):
        auth_token = 'graphlab_awesome'
        tcp_server = start_test_tcp_server(auth_token)

        #launch with remote server tcp address
        glconnect.launch(tcp_server.get_server_addr(), auth_token=auth_token)
        self.assertTrue(glconnect.is_connected())
        self.assertTrue(isinstance(glconnect.get_server(), server.RemoteServer))
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())
        tcp_server.stop()

    @mock.patch('graphlab.connect.main.__LOGGER__')
    def test_launch_with_exception(self, mock_logging):
        ipc_addr = 'ipc://' + tempfile.NamedTemporaryFile().name
        auth_token = 'graphlab_awesome'
        ipc_server = create_server(ipc_addr, auth_token)
        ipc_server.start()

        #default launch without stopping
        glconnect.launch(server_addr=ipc_addr)
        glconnect.launch()
        self.assertTrue(mock_logging.warning.called_once_with(SubstringMatcher(containing="existing server")))
        self.assertTrue(glconnect.is_connected())
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())

        # launch with bogus server path (path is not listend by server)
        with tempfile.NamedTemporaryFile() as f:
            glconnect.launch(server_addr=('ipc://' + f.name))
            self.assertTrue(mock_logging.warning.called_once_with(SubstringMatcher(containing="communication failure")))
            self.assertFalse(glconnect.is_connected())

    @mock.patch('graphlab.connect.main.__LOGGER__')
    def test_secure_communication(self, mock_logging):
        SERVER_PUBLIC_KEY = "Ee4##T$OmI4]hzyKqZT@H&Fixt95^.72&%MK!UR."
        SERVER_SECRET_KEY = "lIn2Szq0.mpPiB<N)t6fR2/4^4&wYnFs-x72HlTz"

        # Non-error case
        ipc_addr = 'ipc://' + tempfile.NamedTemporaryFile().name
        server = create_server(ipc_addr, public_key=SERVER_PUBLIC_KEY, secret_key=SERVER_SECRET_KEY)
        server.start()
        glconnect.launch(server_addr=ipc_addr, server_public_key=SERVER_PUBLIC_KEY)
        self.assertTrue(glconnect.is_connected())
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())

        # Tests with bogus key
        BOGUS_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        ipc_addr = 'ipc://' + tempfile.NamedTemporaryFile().name
        server = create_server(ipc_addr, public_key=BOGUS_KEY, secret_key=SERVER_SECRET_KEY)
        try:
            server.start()
        except:
            pass
        else: 
            self.fail("Server started with bogus key.")
        ipc_addr = 'ipc://' + tempfile.NamedTemporaryFile().name
        server = create_server(ipc_addr, public_key=SERVER_PUBLIC_KEY, secret_key=BOGUS_KEY)
        try:
            server.start()
        except:
            pass
        else: 
            self.fail("Server started with bogus key.")
