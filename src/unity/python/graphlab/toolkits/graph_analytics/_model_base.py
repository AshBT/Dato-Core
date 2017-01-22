"""
Model base for graph analytics models
"""

'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''

import graphlab.connect as _mt
from graphlab.toolkits._model import Model
from prettytable import PrettyTable as _PrettyTable
from graphlab.cython.cy_graph import UnityGraphProxy
from graphlab.cython.cy_sframe import UnitySFrameProxy
import graphlab.toolkits._main as _main
from graphlab.data_structures.sframe import SFrame
from graphlab.data_structures.sgraph import SGraph


class GraphAnalyticsModel(Model):

    def get(self, field):
        """
        Return the value for the queried field.

        Get the value of a given field. The list of all queryable fields is
        documented in the beginning of the model class.

        Each of these fields can be queried in one of two ways:

        >>> out = m['graph']      # m is a trained graph analytics model
        >>> out = m.get('graph')  # equivalent to previous line

        Parameters
        ----------
        field : string
            Name of the field to be retrieved.

        Returns
        -------
        out : value
            The current value of the requested field.

        See Also
        --------
        list_fields
        
        Examples
        --------
        >>> g = m.get('graph')
        """
        _mt._get_metric_tracker().track('toolkit.graph_analytics.get')

        if field in self.list_fields():
            obj = self.__proxy__.get(field)
            if type(obj) == UnityGraphProxy:
                return SGraph(_proxy=obj)
            elif type(obj) == UnitySFrameProxy:
                return SFrame(_proxy=obj)
            else:
                return obj
        else:
            raise KeyError('Key \"%s\" not in model. Available fields are %s.' % (field, ', '.join(self.list_fields())))

    def get_current_options(self):
        """
        Return a dictionary with the options used to define and create this
        graph analytics model instance.

        Returns
        -------
        out : dict
            Dictionary of options used to train this model.

        See Also
        --------
        get_default_options, list_fields, get
        """
        _mt._get_metric_tracker().track('toolkit.graph_analytics.get_current_options')

        dispatch_table = {
            'ShortestPathModel': 'sssp_default_options',
            'GraphColoringModel': 'graph_coloring_default_options',
            'PagerankModel': 'pagerank_default_options',
            'ConnectedComponentsModel': 'connected_components_default_options',
            'TriangleCountingModel': 'triangle_counting_default_options',
            'KcoreModel': 'kcore_default_options'
        }

        try:
            model_options = _main.run(dispatch_table[self.name()], {})

            ## for each of the default options, update its current value by querying the model
            for key in model_options:
                current_value = self.get(key)
                model_options[key] = current_value
            return model_options
        except:
            raise RuntimeError('Model %s does not have options' % self.name())

    def _get_wrapper(self):
        """
        Returns the constructor for the model.
        """
        return lambda m: self.__class__(m)

    @classmethod
    def _describe_fields(cls):
        """
        Return a pretty table for the class fields description.
        """
        dispatch_table = {
            'ShortestPathModel': 'sssp_model_fields',
            'GraphColoringModel': 'graph_coloring_model_fields',
            'PagerankModel': 'pagerank_model_fields',
            'ConnectedComponentsModel': 'connected_components_model_fields',
            'TriangleCountingModel': 'triangle_counting_model_fields',
            'KcoreModel': 'kcore_model_fields'
        }
        try:
            fields_description = _main.run(dispatch_table[cls.__name__], {})
            tbl = _PrettyTable(['Field', 'Description'])
            for k, v in fields_description.iteritems():
                tbl.add_row([k, v])
            tbl.align['Field'] = 'l'
            tbl.align['Description'] = 'l'
            return tbl
        except:
            raise RuntimeError('Model %s does not have fields description' % cls.__name__)

    def _format(self, title, key_values):
        if len(key_values) == 0:
            return ""
        tbl = _PrettyTable(header=False)
        for k, v in key_values.iteritems():
                tbl.add_row([k, v])
        tbl.align['Field 1'] = 'l'
        tbl.align['Field 2'] = 'l'
        s = title + ":\n"
        s += tbl.__str__() + '\n'
        return s

    def __repr__(self):
        g = self['graph']
        s = self.name() + '\n'
        s += self._format('Graph', g.summary())
        s += self._format('Result', self._result_fields())
        s += self._format('Method', self._method_fields())
        return s

    def __str__(self):
        return self.__repr__()

    def summary(self):
        """
        Print a detailed summary of the model.

        Examples
        --------
        >>> m.summary()
        """
        _mt._get_metric_tracker().track('toolkit.graph_analytics.summary')

        s = self.__repr__()
        s += self._format('Setting', self._setting_fields())
        s += self._format('Metric', self._metric_fields())
        s += 'Queriable Fields\n'
        s += self._describe_fields().__str__()
        print s

    def _setting_fields(self):
        "Return model fields related to input setting"
        return {}

    def _method_fields(self):
        "Return model fields related to model methods"
        return {}

    def _result_fields(self):
        return {'graph': "SGraph. See m['graph']"}

    def _metric_fields(self):
        "Return model fields related to training metric"
        return {'training_time': self['training_time']}
