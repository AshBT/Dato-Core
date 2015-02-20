'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''
import graphlab as gl
import os
import unittest
import util
import shutil
import random

temp_number = random.randint(0, 2**64)

class TestModel(unittest.TestCase):
    def __assert_model_equals__(self, m1, m2):
        self.assertEquals(type(m1), type(m2))
        self.assertSequenceEqual(m1.list_fields(), m2.list_fields())

    def __remove_file(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)

    def setUp(self):

        self.pr_model = gl.pagerank.create(gl.SGraph())
        self.cc_model = gl.connected_components.create(gl.SGraph())

        self.__remove_file('./tmp_model-%d' % temp_number)
        self.__remove_file('/tmp/tmp_model-%d' % temp_number)
        self.__remove_file('/tmp/tmp_model2-%d' % temp_number)

    def tearDown(self):
        self.__remove_file('./tmp_model-%d' % temp_number)
        self.__remove_file('/tmp/tmp_model-%d' % temp_number)
        self.__remove_file('/tmp/tmp_model2-%d' % temp_number)

    def test_basic_save_load(self):
        # save and load the pagerank model
        with util.TempDirectory() as tmp_pr_model_file:
            self.pr_model.save(tmp_pr_model_file)
            pr_model2 = gl.load_model(tmp_pr_model_file)
            self.__assert_model_equals__(self.pr_model, pr_model2)

        # save and load the connected_component model
        with util.TempDirectory() as tmp_cc_model_file:
            self.cc_model.save(tmp_cc_model_file)
            cc_model2 = gl.load_model(tmp_cc_model_file)
            self.__assert_model_equals__(self.cc_model, cc_model2)

        # handle different types of urls.
        # TODO: test hdfs and s3 urls.
        for url in ['./tmp_model-%d' % temp_number,
                    '/tmp/tmp_model-%d' % temp_number,
                    'remote:///tmp/tmp_model2-%d' % temp_number]:

            self.pr_model.save(url)
            self.__assert_model_equals__(self.pr_model, gl.load_model(url))

    def test_backwards_compatible_load(self):
        r"""
        `load_model` should be backwards compatible with models saved with the
        previous version of GLC.  Make sure this still works, using a model
        saved from GLC 0.9.

        Notes
        -----
        **Important**: The saved model needs to be updated with every release.
        """

        # Will enable soon.
        # model = gl.load_model('http://s3.amazonaws.com/gl-testdata/mf_model_0.9')
        # assert model is not None


    def test_exception(self):
        # load model from empty file
        with util.TempDirectory() as tmp_empty_file:
            with self.assertRaises(IOError):
                gl.load_model(tmp_empty_file)

        # load model from non-existing file
        if (os.path.exists('./tmp_model-%d' % temp_number)):
            shutil.rmtree('./tmp_model-%d' % temp_number)
        with self.assertRaises(IOError):
            gl.load_model('./tmp_model-%d' % temp_number)

        # save model to invalid url
        for url in ['http://test', '/root/tmp/testmodel']:
            with self.assertRaises(IOError):
                self.pr_model.save(url)
