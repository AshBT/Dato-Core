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
import logging
import os
import unittest
import StringIO 
import ConfigParser

import graphlab.version_info
from graphlab.util import get_newest_version, perform_version_check

class TestVersionCheck(unittest.TestCase):
    """
    This class is to test the version check function defined in graphlab.util.
    """
    def __init__(self, *args, **kwargs):
        super(TestVersionCheck, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.config_file = 'tmp-dummy-config-file'
        self.version_file = 'tmp-dummy-version-file'
        self.version_url = 'file://' + os.path.join(os.getcwd(), self.version_file)

    def setUp(self):
        self._clear_state()
        with open(self.version_file, 'w') as f:
            f.write("0.1.20")

    def tearDown(self):
        self._clear_state()

    def _clear_state(self):
        if (os.path.exists(self.config_file)):
            os.remove(self.config_file)
        if (os.path.exists(self.version_file)):
            os.remove(self.version_file)

    def _clear_config_file(self):
        if (os.path.exists(self.config_file)):
            os.remove(self.config_file)


    def test_get_version(self):
        ret = get_newest_version(_url=self.version_url)
        self.assertEquals(ret, "0.1.20")


    def test_regular_outdated_behavior(self):
        self._clear_config_file()
        graphlab.version_info.version = "0.1.1"
        strout = StringIO.StringIO()
        ret = perform_version_check(configfile=self.config_file,
                                    _url=self.version_url,
                                    _outputstream=strout)
        self.assertEqual(ret, True)
        self.assertNotEqual(strout.getvalue(), "")

    def test_regular_current_behavior(self):
        self._clear_config_file()
        graphlab.version_info.version = "0.1.20"
        strout = StringIO.StringIO()
        ret = perform_version_check(configfile=self.config_file,
                                    _url=self.version_url,
                                    _outputstream=strout)
        self.assertEqual(ret, False)
        self.assertEqual(strout.getvalue(), "")


    def test_regular_overdated_behavior(self):
        self._clear_config_file()
        graphlab.version_info.version = "0.1.30"
        strout = StringIO.StringIO()
        ret = perform_version_check(configfile=self.config_file,
                                    _url=self.version_url,
                                    _outputstream=strout)
        self.assertEqual(ret, False)
        self.assertEqual(strout.getvalue(), "")


    def test_regular_outdated_behavior_with_config_file_skip(self):
        config = ConfigParser.ConfigParser()
        config.add_section("Product")
        config.set('Product', 'skip_version_check', 1)
        with open(self.config_file, 'w') as f:
              config.write(f)
        graphlab.version_info.version = "0.1.1"
        strout = StringIO.StringIO()
        ret = perform_version_check(configfile=self.config_file,
                                    _url=self.version_url,
                                    _outputstream=strout)
        self.assertEqual(ret, False)
        self.assertEqual(strout.getvalue(), "")


    def test_regular_current_behavior_with_config_file_skip(self):
        config = ConfigParser.ConfigParser()
        config.add_section("Product")
        config.set('Product', 'skip_version_check', 1)
        with open(self.config_file, 'w') as f:
              config.write(f)
        graphlab.version_info.version = "0.1.20"
        strout = StringIO.StringIO()
        ret = perform_version_check(configfile=self.config_file,
                                    _url=self.version_url,
                                    _outputstream=strout)
        self.assertEqual(ret, False)
        self.assertEqual(strout.getvalue(), "")



    def test_regular_outdated_behavior_with_config_file_noskip(self):
        config = ConfigParser.ConfigParser()
        config.add_section("Product")
        config.set('Product', 'skip_version_check', 0)
        with open(self.config_file, 'w') as f:
              config.write(f)
        graphlab.version_info.version = "0.1.1"
        strout = StringIO.StringIO()
        ret = perform_version_check(configfile=self.config_file,
                                    _url=self.version_url,
                                    _outputstream=strout)
        self.assertEqual(ret, True)
        self.assertNotEqual(strout.getvalue(), "")


    def test_regular_current_behavior_with_config_file_noskip(self):
        config = ConfigParser.ConfigParser()
        config.add_section("Product")
        config.set('Product', 'skip_version_check', 0)
        with open(self.config_file, 'w') as f:
              config.write(f)
        graphlab.version_info.version = "0.1.20"
        strout = StringIO.StringIO()
        ret = perform_version_check(configfile=self.config_file,
                                    _url=self.version_url,
                                    _outputstream=strout)
        self.assertEqual(ret, False)
        self.assertEqual(strout.getvalue(), "")
