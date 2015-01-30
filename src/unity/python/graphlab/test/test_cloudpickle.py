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
