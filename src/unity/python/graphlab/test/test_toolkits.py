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
import itertools
import tempfile
import util
import unittest

import pandas as pd
from pandas.util.testing import assert_frame_equal

import graphlab as gl
from graphlab.connect.main import get_unity
from graphlab.toolkits._main import ToolkitError
from graphlab.data_structures.sgraph import SGraph
from graphlab.data_structures.sframe import SFrame

dataset_server = "http://testdatasets.s3-website-us-west-2.amazonaws.com/"

class GraphAnalyticsTest(unittest.TestCase):
    def setUp(self):
        url = dataset_server + "p2p-Gnutella04.txt.gz"
        self.graph = gl.load_graph(url, format='snap')

    def __test_model_save_load_helper__(self, model):
        with util.TempDirectory() as f:
            model.save(f)
            m2 = get_unity().load_model(f)
            self.assertItemsEqual(model.list_fields(), m2.list_fields())
            for key in model.list_fields():
                if type(model.get(key)) is SGraph:
                    self.assertItemsEqual(model.get(key).summary(), m2.get(key).summary())
                    self.assertItemsEqual(model.get(key).get_fields(), m2.get(key).get_fields())
                elif type(model.get(key)) is SFrame:
                    sf1 = model.get(key)
                    sf2 = m2.get(key)
                    self.assertEqual(len(sf1), len(sf2))
                    self.assertItemsEqual(sf1.column_names(), sf2.column_names())
                    df1 = sf1.to_dataframe()
                    print df1
                    df2 = sf2.to_dataframe()
                    print df2
                    df1 = df1.set_index(df1.columns[0])
                    df2 = df2.set_index(df2.columns[0])
                    assert_frame_equal(df1, df2)
                else:
                    if (type(model.get(key)) is pd.DataFrame):
                        assert_frame_equal(model.get(key), m2.get(key))
                    else:
                        self.assertEqual(model.get(key), m2.get(key))

    def test_pagerank(self):
        if "pagerank" in get_unity().list_toolkit_functions():
            m = gl.pagerank.create(self.graph)
            print m
            m.summary()
            self.assertEqual((m.get('pagerank').num_rows(), m.get('pagerank').num_cols()),
                             (self.graph.summary()['num_vertices'], 3))
            self.assertAlmostEqual(m['pagerank']['pagerank'].sum(), 2727.5348, delta=1e-3)
            self.__test_model_save_load_helper__(m)

            m2 = gl.pagerank.create(self.graph, reset_probability=0.5)
            print m2
            self.assertEqual((m2.get('pagerank').num_rows(), m2.get('pagerank').num_cols()),
                             (self.graph.summary()['num_vertices'], 3))
            self.assertAlmostEqual(m2['pagerank']['pagerank'].sum(), 7087.0791, delta=1e-3)
            with self.assertRaises(Exception):
                assert_frame_equal(m.get('pagerank').topk('pagerank'), m2.get('pagerank').topk('pagerank'))
            self.__test_model_save_load_helper__(m2)

            default_options = gl.pagerank.get_default_options()
            print default_options
            self.assertTrue(len(default_options.keys()) == 3)
            self.assertTrue(default_options['reset_probability'] == 0.15)
            self.assertTrue(default_options['threshold'] == 1e-2)
            self.assertTrue(default_options['max_iterations'] == 20)

            current_options = m2.get_current_options()
            print current_options
            self.assertTrue(len(current_options.keys()) == 3)
            self.assertTrue(current_options['reset_probability'] == 0.5)
            self.assertTrue(current_options['threshold'] == 1e-2)
            self.assertTrue(current_options['max_iterations'] == 20)

    def test_triangle_counting(self):
        if "triangle_counting" in get_unity().list_toolkit_functions():
            m = gl.triangle_counting.create(self.graph)
            print m
            m.summary()
            self.__test_model_save_load_helper__(m)
            self.assertEqual(m.get('num_triangles'), 934)

            default_options = gl.triangle_counting.get_default_options()
            self.assertTrue(len(default_options.keys()) == 0)

            current_options = m.get_current_options()
            self.assertTrue(len(current_options.keys()) == 0)

    def test_connected_component(self):
        if "connected_component" in get_unity().list_toolkit_functions():
            m = gl.connected_components.create(self.graph)
            print m
            m.summary()
            print m.get('component_id')
            print m.get('component_size')
            self.assertEqual(m['component_size'].num_rows(), 1)
            self.__test_model_save_load_helper__(m)

            default_options = gl.connected_components.get_default_options()
            self.assertTrue(len(default_options.keys()) == 0)

            current_options = m.get_current_options()
            self.assertTrue(len(current_options.keys()) == 0)

    def test_graph_coloring(self):
        if "graph_coloring" in get_unity().list_toolkit_functions():
            m = gl.graph_coloring.create(self.graph)
            print m
            m.summary()
            # coloring is non-deterministic, so we cannot verify the result here
            self.__test_model_save_load_helper__(m)

            default_options = gl.graph_coloring.get_default_options()
            self.assertTrue(len(default_options.keys()) == 0)

            current_options = m.get_current_options()
            self.assertTrue(len(current_options.keys()) == 0)

    def test_kcore(self):
        if "kcore" in get_unity().list_toolkit_functions():
            m = gl.kcore.create(self.graph)
            print m
            m.summary()
            biggest_core = m['core_id'].groupby('core_id', gl.aggregate.COUNT).topk('Count').head(1)
            self.assertEqual(biggest_core['core_id'][0], 6)
            self.assertEqual(biggest_core['Count'][0], 4492)
            self.__test_model_save_load_helper__(m)

            default_options = gl.kcore.get_default_options()
            print default_options
            self.assertTrue(len(default_options.keys()) == 2)
            self.assertTrue(default_options['kmin'] == 0)
            self.assertTrue(default_options['kmax'] == 10)

            m2 = gl.kcore.create(self.graph, kmin = 1, kmax = 5)
            current_options = m2.get_current_options()
            print current_options
            self.assertTrue(len(current_options.keys()) == 2)
            self.assertTrue(current_options['kmin'] == 1)
            self.assertTrue(current_options['kmax'] == 5)

    def test_shortest_path(self):
        if "sssp" in get_unity().list_toolkit_functions():
            m = gl.shortest_path.create(self.graph, source_vid=0)
            print m
            m.summary()
            self.__test_model_save_load_helper__(m)

            m2 = gl.shortest_path.create(self.graph, source_vid=0)
            print m2
            self.__test_model_save_load_helper__(m2)

            # Test get_path function on a simple chain graph and star graph
            chain_graph = gl.SGraph().add_edges([gl.Edge(i, i + 1) for i in range(10)])
            m3 = gl.shortest_path.create(chain_graph, source_vid=0)
            for i in range(10):
                self.assertSequenceEqual(m3.get_path(i), [(j, float(j)) for j in range(i + 1)])

            star_graph = gl.SGraph().add_edges([gl.Edge(0, i + 1) for i in range(10)])
            m4 = gl.shortest_path.create(star_graph, source_vid=0)
            for i in range(1, 11):
                self.assertSequenceEqual(m4.get_path(i), [(0, 0.0), (i, 1.0)])

            # Test sssp ignoring the existing distance field
            star_graph.vertices['distance'] = 0
            m5 = gl.shortest_path.create(star_graph, source_vid=0)
            for i in range(1, 11):
                self.assertSequenceEqual(m5.get_path(i), [(0, 0.0), (i, 1.0)])

            default_options = gl.shortest_path.get_default_options()
            print default_options
            self.assertTrue(len(default_options.keys()) == 2)
            self.assertTrue(default_options['weight_field'] == "")
            self.assertTrue(default_options['max_distance'] == 1e30)

            m6 = gl.shortest_path.create(chain_graph, source_vid=0, max_distance=3)
            current_options = m6.get_current_options()
            print current_options
            self.assertTrue(len(current_options.keys()) == 2)
            self.assertTrue(current_options['weight_field'] == "")
            self.assertTrue(current_options['max_distance'] == 3)

    def test_compute_shortest_path(self):
            edge_src_ids = ['src1', 'src2',   'a', 'b', 'c'  ]
            edge_dst_ids = [   'a',    'b', 'dst', 'c', 'dst']
            edges = gl.SFrame({'__src_id': edge_src_ids, '__dst_id': edge_dst_ids})
            g=gl.SGraph().add_edges(edges)
            res = list(gl.shortest_path._compute_shortest_path(g, ["src1","src2"], "dst"))
            self.assertEquals(res, [["src1", "a", "dst"]])
            res = list(gl.shortest_path._compute_shortest_path(g, "src2", "dst"))
            self.assertEquals(res, [["src2", "b", "c", "dst"]])

            edge_src_ids = [0,1,2,3,4]
            edge_dst_ids = [2,3,5,4,5]
            edge_weights   = [1,0.1,1,0.1,0.1]
            g=gl.SFrame({'__src_id':edge_src_ids,'__dst_id':edge_dst_ids, 'weights':edge_weights})
            g=gl.SGraph(edges=g)
            t=gl.shortest_path._compute_shortest_path(g,[0,1],[5],"weights")
            self.assertEquals(t, [1,3,4,5])
