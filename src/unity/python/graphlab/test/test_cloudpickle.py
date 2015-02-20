'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''
import graphlab as gl
import unittest
from pickle import PicklingError
import graphlab_util.cloudpickle as cloudpickle


class CloudPicleTest(unittest.TestCase):

    def test_pickle_unity_object_exception(self):
        sa = gl.SArray()
        sf = gl.SFrame()
        g = gl.SGraph()
        sk = sa.sketch_summary()
        m = gl.pagerank.create(g)
        for obj in [sa, sf, g, sk, m]:
            self.assertRaises(PicklingError, lambda: cloudpickle.dumps(obj))
