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
import commands
import json
import logging
import os
import re
import tempfile
import unittest
import pandas

import graphlab
import graphlab.connect.main as glconnect
import graphlab.sys_util as _sys_util
from graphlab.test.util import create_server, start_test_tcp_server
from pandas.util.testing import assert_frame_equal


def _test_save_load_object_helper(testcase, obj, url):
    """
    Helper function to test save and load a server side object to a given url.
    """
    def cleanup(url):
        """
        Remove the saved file from temp directory.
        """
        protocol = None
        path = None
        splits = url.split("://")
        if len(splits) > 1:
            protocol = splits[0]
            path = splits[1]
        else:
            path = url
        if not protocol or protocol is "local" or protocol is "remote":
            tempdir = tempfile.gettempdir()
            pattern = path + ".*"
            for f in os.listdir(tempdir):
                if re.search(pattern, f):
                    os.remove(os.path.join(tempdir, f))

    if isinstance(obj, graphlab.SGraph):
        obj.save(url + ".graph")
        newobj = graphlab.load_graph(url + ".graph")
        testcase.assertItemsEqual(obj.get_fields(), newobj.get_fields())
        testcase.assertDictEqual(obj.summary(), newobj.summary())
    elif isinstance(obj, graphlab.Model):
        obj.save(url + ".model")
        newobj = graphlab.load_model(url + ".model")
        testcase.assertItemsEqual(obj.list_fields(), newobj.list_fields())
        testcase.assertEqual(type(obj), type(newobj))
    elif isinstance(obj, graphlab.SFrame):
        obj.save(url + ".frame_idx")
        newobj = graphlab.load_sframe(url + ".frame_idx")
        testcase.assertEqual(obj.shape, newobj.shape)
        testcase.assertEqual(obj.column_names(), newobj.column_names())
        testcase.assertEqual(obj.column_types(), newobj.column_types())
        assert_frame_equal(obj.head(obj.num_rows()).to_dataframe(),
                           newobj.head(newobj.num_rows()).to_dataframe())
    else:
        raise TypeError
    cleanup(url)


def create_test_objects():
    vertices = pandas.DataFrame({'vid': ['1', '2', '3'],
                                 'color': ['g', 'r', 'b'],
                                 'vec': [[.1, .1, .1], [.1, .1, .1], [.1, .1, .1]]})
    edges = pandas.DataFrame({'src_id': ['1', '2', '3'],
                              'dst_id': ['2', '3', '4'],
                              'weight': [0., 0.1, 1.]})

    graph = graphlab.SGraph().add_vertices(vertices, 'vid').add_edges(edges, 'src_id', 'dst_id')
    sframe = graphlab.SFrame(edges)
    model = graphlab.pagerank.create(graph)
    return (graph, sframe, model)


class LocalFSConnectorTests(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.tempfile = tempfile.NamedTemporaryFile().name
        (self.graph, self.sframe, self.model) = create_test_objects()

    def _test_read_write_helper(self, url, content):
        url = graphlab.util.make_internal_url(url)
        glconnect.get_unity().__write__(url, content)
        content_read = glconnect.get_unity().__read__(url)
        self.assertEquals(content_read, content)
        if os.path.exists(url):
            os.remove(url)

    def test_object_save_load(self):
        for prefix in ['', 'local://', 'remote://']:
            _test_save_load_object_helper(self, self.graph, prefix + self.tempfile)
            _test_save_load_object_helper(self, self.model, prefix + self.tempfile)
            _test_save_load_object_helper(self, self.sframe, prefix + self.tempfile)

    def test_basic(self):
        self._test_read_write_helper(self.tempfile, 'hello world')
        self._test_read_write_helper("local://" + self.tempfile + ".csv", 'hello,world,woof')
        self._test_read_write_helper("remote://" + self.tempfile + ".csv", 'hello,world,woof')

    def test_gzip(self):
        self._test_read_write_helper(self.tempfile + ".gz", 'hello world')
        self._test_read_write_helper(self.tempfile + ".csv.gz", 'hello world')
        self._test_read_write_helper("local://" + self.tempfile + ".csv.gz", 'hello world')
        self._test_read_write_helper("remote://" + self.tempfile + ".csv.gz", 'hello world')

    def test_exception(self):
        self.assertRaises(IOError, lambda: glconnect.get_unity().__read__("/root/tmp"))
        self.assertRaises(IOError, lambda: glconnect.get_unity().__write__("/root/tmp", '.....'))
        self.assertRaises(IOError, lambda: glconnect.get_unity().__read__("/root/tmp"))
        self.assertRaises(IOError, lambda: glconnect.get_unity().__write__("/root/tmp", '.....'))
        self.assertRaises(IOError, lambda: self.graph.save("/root/tmp.graph"))
        self.assertRaises(IOError, lambda: self.sframe.save("/root/tmp.frame_idx"))
        self.assertRaises(IOError, lambda: self.model.save("/root/tmp.model"))
        self.assertRaises(IOError, lambda: graphlab.load_graph("/root/tmp.graph"))
        self.assertRaises(IOError, lambda: graphlab.load_sframe("/root/tmp.frame_idx"))
        self.assertRaises(IOError, lambda: graphlab.load_model("/root/tmp.model"))


class RemoteFSConnectorTests(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        glconnect.stop()
        auth_token = 'graphlab_awesome'
        self.server = start_test_tcp_server(auth_token=auth_token)
        glconnect.launch(self.server.get_server_addr(), auth_token=auth_token)
        self.tempfile = tempfile.NamedTemporaryFile().name
        (self.graph, self.sframe, self.model) = create_test_objects()

    @classmethod
    def tearDownClass(self):
        glconnect.stop()
        self.server.stop()

    def _test_read_write_helper(self, url, content):
        url = graphlab.util.make_internal_url(url)
        glconnect.get_unity().__write__(url, content)
        content_read = glconnect.get_unity().__read__(url)
        self.assertEquals(content_read, content)

    def test_basic(self):
        self._test_read_write_helper("remote://" + self.tempfile, 'hello,world,woof')

    def test_gzip(self):
        self._test_read_write_helper("remote://" + self.tempfile + ".csv.gz", 'hello,world,woof')

    def test_object_save_load(self):
        prefix = "remote://"
        _test_save_load_object_helper(self, self.graph, prefix + self.tempfile)
        _test_save_load_object_helper(self, self.model, prefix + self.tempfile)
        _test_save_load_object_helper(self, self.sframe, prefix + self.tempfile)

    def test_exception(self):
        self.assertRaises(ValueError, lambda: self._test_read_write_helper(self.tempfile, 'hello world'))
        self.assertRaises(ValueError, lambda: self._test_read_write_helper("local://" + self.tempfile + ".csv.gz", 'hello,world,woof'))
        self.assertRaises(IOError, lambda: glconnect.get_unity().__read__("remote:///root/tmp"))
        self.assertRaises(IOError, lambda: glconnect.get_unity().__read__("remote:///root/tmp"))
        self.assertRaises(IOError, lambda: glconnect.get_unity().__write__("remote:///root/tmp", '.....'))
        self.assertRaises(IOError, lambda: self.graph.save("remote:///root/tmp.graph"))
        self.assertRaises(IOError, lambda: self.sframe.save("remote:///root/tmp.frame_idx"))
        self.assertRaises(IOError, lambda: self.model.save("remote:///root/tmp.model"))
        self.assertRaises(IOError, lambda: graphlab.load_graph("remote:///root/tmp.graph"))
        self.assertRaises(IOError, lambda: graphlab.load_sframe("remote:///root/tmp.frame_idx"))
        self.assertRaises(IOError, lambda: graphlab.load_model("remote:///root/tmp.model"))


class HttpConnectorTests(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.url = "http://s3-us-west-2.amazonaws.com/testdatasets/a_to_z.txt.gz"

    def _test_read_helper(self, url, content_expected):
        url = graphlab.util.make_internal_url(url)
        content_read = glconnect.get_unity().__read__(url)
        self.assertEquals(content_read, content_expected)

    def test_read(self):
        expected = "\n".join([str(unichr(i + ord('a'))) for i in range(26)])
        expected = expected + "\n"
        self._test_read_helper(self.url, expected)

    def test_exception(self):
        self.assertRaises(IOError, lambda: glconnect.get_unity().__write__(self.url, '.....'))

@unittest.skip("Disabling HDFS Connector Tests")
class HDFSConnectorTests(unittest.TestCase):
    # This test requires hadoop to be installed and avaiable in $PATH.
    # If not, the tests will be skipped.
    @classmethod
    def setUpClass(self):
        self.has_hdfs = len(_sys_util.get_hadoop_class_path()) > 0
        self.tempfile = tempfile.NamedTemporaryFile().name
        (self.graph, self.sframe, self.model) = create_test_objects()

    def _test_read_write_helper(self, url, content_expected):
        url = graphlab.util.make_internal_url(url)
        glconnect.get_unity().__write__(url, content_expected)
        content_read = glconnect.get_unity().__read__(url)
        self.assertEquals(content_read, content_expected)
        # clean up the file we wrote
        status, output = commands.getstatusoutput('hadoop fs -test -e ' + url)
        if status is 0:
            commands.getstatusoutput('hadoop fs -rm ' + url)

    def test_basic(self):
        if self.has_hdfs:
            self._test_read_write_helper("hdfs://" + self.tempfile, 'hello,world,woof')
        else:
            logging.getLogger(__name__).info("No hdfs avaiable. Test pass.")

    def test_gzip(self):
        if self.has_hdfs:
            self._test_read_write_helper("hdfs://" + self.tempfile + ".gz", 'hello,world,woof')
            self._test_read_write_helper("hdfs://" + self.tempfile + ".csv.gz", 'hello,world,woof')
        else:
            logging.getLogger(__name__).info("No hdfs avaiable. Test pass.")

    def test_object_save_load(self):
        if self.has_hdfs:
            prefix = "hdfs://"
            _test_save_load_object_helper(self, self.graph, prefix + self.tempfile)
            _test_save_load_object_helper(self, self.model, prefix + self.tempfile)
            _test_save_load_object_helper(self, self.sframe, prefix + self.tempfile)
        else:
            logging.getLogger(__name__).info("No hdfs avaiable. Test pass.")

    def test_exception(self):
        bad_url = "hdfs:///root/"
        if self.has_hdfs:
            self.assertRaises(IOError, lambda: glconnect.get_unity().__read__("hdfs:///"))
            self.assertRaises(IOError, lambda: glconnect.get_unity().__read__("hdfs:///tmp"))
            self.assertRaises(IOError, lambda: glconnect.get_unity().__read__("hdfs://" + self.tempfile))
            self.assertRaises(IOError, lambda: glconnect.get_unity().__write__(bad_url + "/tmp", "somerandomcontent"))
            self.assertRaises(IOError, lambda: self.graph.save(bad_url + "x.graph"))
            self.assertRaises(IOError, lambda: self.sframe.save(bad_url + "x.frame_idx"))
            self.assertRaises(IOError, lambda: self.model.save(bad_url + "x.model"))
            self.assertRaises(IOError, lambda: graphlab.load_graph(bad_url + "mygraph"))
            self.assertRaises(IOError, lambda: graphlab.load_sframe(bad_url + "x.frame_idx"))
            self.assertRaises(IOError, lambda: graphlab.load_model(bad_url + "x.model"))
        else:
            logging.getLogger(__name__).info("No hdfs avaiable. Test pass.")


@unittest.skip("Disabling S3 Connector Tests")
class S3ConnectorTests(unittest.TestCase):
    # This test requires aws cli to be installed. If not, the tests will be skipped.
    @classmethod
    def setUpClass(self):
        status, output = commands.getstatusoutput('aws s3api list-buckets')
        self.has_s3 = (status is 0)
        self.standard_bucket = None
        self.regional_bucket = None
        # Use aws cli s3api to find a bucket with "gl-testdata" in the name, and use it as out test bucket.
        # Temp files will be read from /written to the test bucket's /tmp folder and be cleared on exist.
        if self.has_s3:
            try:
                json_output = json.loads(output)
                bucket_list = [b['Name'] for b in json_output['Buckets']]
                assert 'gl-testdata' in bucket_list
                assert 'gl-testdata-oregon' in bucket_list
                self.standard_bucket = 'gl-testdata'
                self.regional_bucket = 'gl-testdata-oregon'
                self.tempfile = tempfile.NamedTemporaryFile().name
                (self.graph, self.sframe, self.model) = create_test_objects()
            except:
                logging.getLogger(__name__).warning("Fail parsing ioutput of s3api into json. Please check your awscli version.")
                self.has_s3 = False

    def _test_read_write_helper(self, url, content_expected):
        s3url = graphlab.util.make_internal_url(url)
        glconnect.get_unity().__write__(s3url, content_expected)
        content_read = glconnect.get_unity().__read__(s3url)
        self.assertEquals(content_read, content_expected)
        (status, output) = commands.getstatusoutput('aws s3 rm --region us-west-2 ' + url)
        if status is not 0:
            logging.getLogger(__name__).warning("Cannot remove file: " + url)

    def test_basic(self):
        if self.has_s3:
            for bucket in [self.standard_bucket, self.regional_bucket]:
                self._test_read_write_helper("s3://" + bucket + self.tempfile, 'hello,world,woof')
        else:
            logging.getLogger(__name__).info("No s3 bucket avaiable. Test pass.")

    def test_gzip(self):
        if self.has_s3:
            self._test_read_write_helper("s3://" + self.standard_bucket + self.tempfile + ".gz", 'hello,world,woof')
        else:
            logging.getLogger(__name__).info("No s3 bucket avaiable. Test pass.")

    def test_object_save_load(self):
        if self.has_s3:
            prefix = "s3://" + self.standard_bucket
            _test_save_load_object_helper(self, self.graph, prefix + self.tempfile)
            _test_save_load_object_helper(self, self.model, prefix + self.tempfile)
            _test_save_load_object_helper(self, self.sframe, prefix + self.tempfile)
        else:
            logging.getLogger(__name__).info("No s3 bucket avaiable. Test pass.")

    def test_exception(self):
        if self.has_s3:
            bad_bucket = "i_am_a_bad_bucket"
            prefix = "s3://" + bad_bucket
            self.assertRaises(IOError, lambda: glconnect.get_unity().__read__("s3:///"))
            self.assertRaises(IOError, lambda: glconnect.get_unity().__read__("s3://" + self.standard_bucket + "/somerandomfile"))
            self.assertRaises(IOError, lambda: glconnect.get_unity().__read__("s3://" + "/somerandomfile"))
            self.assertRaises(IOError, lambda: glconnect.get_unity().__write__("s3://" + "/somerandomfile", "somerandomcontent"))
            self.assertRaises(IOError, lambda: glconnect.get_unity().__write__("s3://" + self.standard_bucket + "I'amABadUrl/", "somerandomcontent"))
            self.assertRaises(IOError, lambda: self.graph.save(prefix + "/x.graph"))
            self.assertRaises(IOError, lambda: self.sframe.save(prefix + "/x.frame_idx"))
            self.assertRaises(IOError, lambda: self.model.save(prefix + "/x.model"))
            self.assertRaises(IOError, lambda: graphlab.load_graph(prefix + "/x.graph"))
            self.assertRaises(IOError, lambda: graphlab.load_sframe(prefix + "/x.frame_idx"))
            self.assertRaises(IOError, lambda: graphlab.load_model(prefix + "/x.model"))
        else:
            logging.getLogger(__name__).info("No s3 bucket avaiable. Test pass.")
