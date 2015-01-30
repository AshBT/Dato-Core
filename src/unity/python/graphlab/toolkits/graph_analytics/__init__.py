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
""" 
The graph analytics toolkit contains methods for analyzing a
:class:`~graphlab.SGraph`. Each method takes an input graph and returns a model
object, which contains the training time, an :class:`~graphlab.SFrame` with the
desired output for each vertex, and a new graph whose vertices contain the
output as an attribute. Note that all of the methods in the graph analytics
toolkit are available from the top level graphlab import.

For this example we download a dataset of James Bond characters to an
SFrame, then build the SGraph.

.. sourcecode:: python

    >>> from graphlab import SFrame, SGraph
    >>> url = 'http://s3.amazonaws.com/dato-datasets/bond/bond_edges.csv'
    >>> data = graphlab.SFrame.read_csv(url)
    >>> g = SGraph().add_edges(data, src_field='src', dst_field='dst')

The :py:func:`connected_components.create()
<graphlab.connected_components.create>` method finds all weakly connected
components in the graph and returns a
:py:class:`ConnectedComponentsModel<graphlab.connected_components.ConnectedComponentsModel>`.
A connected component is a group of vertices such that
there is a path between each vertex in the component and all other vertices in
the group. If two vertices are in different connected components there is no
path between them.

.. sourcecode:: python

    >>> from graphlab import connected_components
    >>> cc = connected_components.create(g)
    >>> print cc.summary()
    >>> print cc.list_fields()

    >>> cc_ids = cc.get('component_id')  # an SFrame
    >>> cc_ids = cc['component_id']      # equivalent to the above line
    >>> cc_graph = cc['graph']  # a GraphLab Create SGraph

The :py:func:`shortest_path.create() <graphlab.shortest_path.create>`
method finds the shortest directed path distance from a single source vertex to
all other vertices and returns a
:py:class:`ShortestPathModel<graphlab.shortest_path.ShortestPathModel>`.
The output model contains this information and a method to
retrieve the actual path to a particular vertex.

.. sourcecode:: python

    >>> from graphlab import shortest_path
    >>> sp = shortest_path.create(g, source_vid=123)
    >>> sp_sframe = sp['distance']
    >>> sp_graph = sp['graph']
    >>> path = sp.get_path('98')

The :py:func:`triangle_counting.create() <graphlab.triangle_counting.create>`
counts the number of triangles in the graph and for each vertex and returns
a :py:class:`TriangleCountingModel<graphlab.triangle_counting.TriangleCountingModel>`.
A graph triangle is a complete subgraph of three vertices. The number of
triangles to which a vertex belongs is an indication of the connectivity of the
graph around that vertex.

.. sourcecode:: python

    >>> from graphlab import triangle_counting
    >>> tc = triangle_counting.create(g)
    >>> tc_out = tc['triangle_count']

The :py:func:`pagerank.create() <graphlab.pagerank.create>` method compuates
the pagerank for each vertex and returns a :py:class:`PagerankModel<graphlab.pagerank.PagerankModel>`.
The pagerank value indicates the centrality of each node in the graph.

.. sourcecode:: python

    >>> from graphlab import pagerank
    >>> pr = pagerank.create(g)
    >>> pr_out = pr['pagerank']

The K-core decomposition recursively removes vertices from the graph with degree
less than k. The value of k where a vertex is removed is its core ID; the
:py:func:`kcore.create() <graphlab.kcore.create>` method returns
a :py:class:`KcoreModel<graphlab.kcore.KcoreModel>` which contains the core ID for
every vertex in the graph.

.. sourcecode:: python

    >>> from graphlab import kcore
    >>> kc = kcore.create(g)
    >>> kcore_id = kc['core_id']

Graph coloring assigns each vertex in the graph to a group in such a way that no
two adjacent vertices share the same group.
:py:func:`graph_coloring.create() <graphlab.graph_coloring.create>` method returns
a :py:class:`GraphColoringModel<graphlab.graph_coloring.GraphColoringModel>` which contains
the color group ID for every vertex in the graph.

.. sourcecode:: python

    >>> from graphlab import graph_coloring
    >>> color = graph_coloring.create(g)
    >>> color_id = color['color_id']
    >>> num_colors = color['num_colors']

For more information on the models in the graph analytics toolkit, plus extended
examples, please see the model definitions and create methods in the API
documentation, as well as the data science `Gallery
<https://dato.com/learn/gallery>`_, the `How-Tos
<https://dato.com/learn/how-to>`_, and the `graph analytics chapter of
the User Guide
<https://dato.com/learn/userguide/index.html#Modeling_data_Graph_analytics>`_.
"""

import pagerank
import triangle_counting
import connected_components
import kcore
import graph_coloring
import shortest_path
