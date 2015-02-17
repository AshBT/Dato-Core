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
##\internal
"""@package graphlab.toolkits

This module defines the (internal) utility functions used by the toolkits.
"""

import json
from graphlab.data_structures.sframe import SArray as _SArray
from graphlab.data_structures.sframe import SFrame as _SFrame
from graphlab.data_structures.sgraph import SGraph as _SGraph
from graphlab.data_structures.sgraph import Vertex as _Vertex
from graphlab.data_structures.sgraph import Edge as _Edge
from graphlab.cython.cy_sarray import UnitySArrayProxy
from graphlab.cython.cy_sframe import UnitySFrameProxy
from graphlab.cython.cy_graph import UnityGraphProxy
from graphlab.toolkits._main import ToolkitError
import logging as _logging


_proxy_map = {UnitySFrameProxy: (lambda x: _SFrame(_proxy=x)),
              UnitySArrayProxy: (lambda x: _SArray(_proxy=x)),
              UnityGraphProxy: (lambda x: _SGraph(_proxy=x))}

def _add_docstring(format_dict):
  """
  Format a doc-string on the fly.
  @arg format_dict: A dictionary to format the doc-strings
  Example:

    @add_docstring({'context': __doc_string_context})
    def predict(x):
      '''
      {context}
        >> model.predict(data)
      '''
      return x
  """
  def add_docstring_context(func):
      def wrapper(*args, **kwargs):
          return func(*args, **kwargs)
      wrapper.__doc__ = func.__doc__.format(**format_dict)
      return wrapper
  return add_docstring_context


def _SGraphFromJsonTree(json_str):
    """
    Convert the Json Tree to SGraph
    """
    g = json.loads(json_str)
    vertices = [_Vertex(x['id'],
                dict([(str(k), v) for k, v in x.iteritems() if k != 'id']))
                                                      for x in g['vertices']]
    edges = [_Edge(x['src'], x['dst'],
             dict([(str(k), v) for k, v in x.iteritems() if k != 'src' and k != 'dst']))
                                                      for x in g['edges']]
    sg = _SGraph().add_vertices(vertices)
    if len(edges) > 0:
        sg = sg.add_edges(edges)
    return sg

class _precomputed_field(object):
    def __init__(self, field):
        self.field = field

def _toolkit_repr_print(model, fields, width=20, section_titles=None):
    """
    Display a toolkit repr according to some simple rules.

    Parameters
    ----------
    model : GraphLab Create model

    fields: List of lists of tuples
        Each tuple should be (display_name, field_name), where field_name can
        be a string or a _precomputed_field object.


    section_titles: List of section titles, one per list in the fields arg.

    Example
    -------

        model_fields = [
            ("L1 penalty", 'l1_penalty'),
            ("L2 penalty", 'l2_penalty'),
            ("Examples", 'num_examples'),
            ("Features", 'num_features'),
            ("Coefficients", 'num_coefficients')]

        solver_fields = [
            ("Solver", 'solver'),
            ("Solver iterations", 'training_iterations'),
            ("Solver status", 'training_solver_status'),
            ("Training time (sec)", 'training_time')]

        training_fields = [
            ("Log-likelihood", 'training_loss')]

        fields = [model_fields, solver_fields, training_fields]:

        section_titles = ['Model description',
                          'Solver description',
                          'Training information']

        _toolkit_repr_print(model, fields, section_titles)
    """
    key_str = "{:<{}}: {}"

    ret = []
    ret.append(key_str.format("Class", width, model.__class__.__name__) + '\n')
    if section_titles is not None:
        assert len(section_titles) == len(fields), \
        "The number of section titles ({0}) ".format(len(section_titles)) +\
        "doesn't match the number of groups of fields, {0}.".format(len(fields))

    for i in range(len(fields)):
        if section_titles is not None:
            ret.append(section_titles[i])
            bar = '-' * len(section_titles[i])
            ret.append(bar)
        for k, v in fields[i]:
            if isinstance(v, _precomputed_field):
                value = v.field
            else:
                value = model.get(v)
            if isinstance(value, float):
                try:
                    value = round(value, 4)
                except:
                    pass
            ret.append(key_str.format(k, width, value))
        ret.append("")
    return '\n'.join(ret)

def _map_unity_proxy_to_object(value):
    """
    Map returning value, if it is unity SFrame, SArray, map it
    """
    vtype = type(value)
    if vtype in _proxy_map:
        return _proxy_map[vtype](value)
    elif vtype == list:
        return [_map_unity_proxy_to_object(v) for v in value]
    elif vtype == dict:
        return {k:_map_unity_proxy_to_object(v) for k,v in value.items()}
    else:
        return value

def _toolkits_select_columns(dataset, columns):
    """
    Same as select columns but redirect runtime error to ToolkitError.
    """
    try:
        return dataset.select_columns(columns)
    except RuntimeError as e:
        raise ToolkitError, str(e)

def _raise_error_if_column_exists(dataset, column_name = 'dataset',
                            dataset_variable_name = 'dataset',
                            column_name_error_message_name = 'column_name'):
    """
    Check if a column exists in an SFrame with error message.
    """
    err_msg = 'The SFrame {0} must contain the column {1}.'.format(
                                                dataset_variable_name,
                                             column_name_error_message_name)
    if column_name not in dataset.column_names():
      raise ToolkitError, str(err_msg)

def _check_categorical_option_type(option_name, option_value, possible_values):
    """
    Check whether or not the requested option is one of the allowed values.
    """
    err_msg = '{0} is not a valid option for {0}. '.format(option_value, option_name)
    err_msg += ' Expected one of: '.format(possible_values)

    err_msg += ', '.join(possible_values)
    if option_value not in possible_values:
        raise ToolkitError, err_msg

def _raise_error_if_not_sarray(dataset, variable_name="SFrame"):
    """
    Check if the input is an SArray. Provide a proper error
    message otherwise.
    """
    err_msg = "Input %s is not an SArray."
    if not isinstance(dataset, _SArray):
      raise ToolkitError, err_msg % variable_name

def _raise_error_if_not_sframe(dataset, variable_name="SFrame"):
    """
    Check if the input is an SFrame. Provide a proper error
    message otherwise.
    """
    err_msg = "Input %s is not an SFrame. If it is a Pandas DataFrame,"
    err_msg += " you may use the to_sframe() function to convert it to an SFrame."

    if not isinstance(dataset, _SFrame):
      raise ToolkitError, err_msg % variable_name

def _raise_error_if_sframe_empty(dataset, variable_name="SFrame"):
    """
    Check if the input is empty.
    """
    err_msg = "Input %s either has no rows or no columns. A non-empty SFrame "
    err_msg += "is required."

    if dataset.num_rows() == 0 or dataset.num_cols() == 0:
        raise ToolkitError, err_msg % variable_name

def _raise_error_if_not_iterable(dataset, variable_name="SFrame"):
    """
    Check if the input is iterable.
    """
    err_msg = "Input %s is not iterable: hasattr(%s, '__iter__') must be true."

    if not hasattr(dataset, '__iter__'):
        raise ToolkitError, err_msg % variable_name


def _raise_error_evaluation_metric_is_valid(metric, allowed_metrics):
    """
    Check if the input is an SFrame. Provide a proper error
    message otherwise.
    """

    err_msg = "Evaluation metric '%s' not recognized. The supported evaluation"
    err_msg += " metrics are (%s)."

    if metric not in allowed_metrics:
      raise ToolkitError, err_msg % (metric,
                          ', '.join(map(lambda x: "'%s'" % x, allowed_metrics)))

def _select_valid_features(dataset, features, valid_feature_types,
                           target_column=None):
    """
    Utility function for selecting columns of only valid feature types.

    Parameters
    ----------
    dataset: SFrame
        The input SFrame containing columns of potential features.

    features: list[str]
        List of feature column names.  If None, the candidate feature set is
        taken to be all the columns in the dataset.

    valid_feature_types: list[type]
        List of Python types that represent valid features.  If type is array.array,
        then an extra check is done to ensure that the individual elements of the array
        are of numeric type.  If type is dict, then an extra check is done to ensure
        that dictionary values are numeric.

    target_column: str
        Name of the target column.  If not None, the target column is excluded
        from the list of valid feature columns.

    Returns
    -------
    out: list[str]
        List of valid feature column names.  Warnings are given for each candidate
        feature column that is excluded.

    Examples
    --------
    # Select all the columns of type `str` in sf, excluding the target column named
    # 'rating'
    >>> valid_columns = _select_valid_features(sf, None, [str], target_column='rating')

    # Select the subset of columns 'X1', 'X2', 'X3' that has dictionary type or defines
    # numeric array type
    >>> valid_columns = _select_valid_features(sf, ['X1', 'X2', 'X3'], [dict, array.array])
    """
    if features is not None:
        if not hasattr(features, '__iter__'):
            raise TypeError("Input 'features' must be an iterable type.")

        if not all([isinstance(x, str) for x in features]):
            raise TypeError("Input 'features' must contain only strings.")

    ## Extract the features and labels
    if features is None:
        features = dataset.column_names()

    col_type_map = {
        col_name: col_type for (col_name, col_type) in
        zip(dataset.column_names(), dataset.column_types()) }

    valid_features = []
    for col_name in features:

        if col_name == target_column:
            _logging.warning("Excluding target column " + target_column +\
                             " as a feature.")

        else:
            if col_type_map[col_name] in valid_feature_types:
                valid_features.append(col_name)
            else:
                _logging.warning("Column " + col_name +
                    " is excluded as a feature due to invalid column type: " +
                    str(col_type_map.get(col_name)) + ".\n")

    if len(valid_features) == 0:
        raise ValueError("The dataset does not contain any valid feature columns. " +
            "Accepted feature types are " + str(valid_feature_types) + ".")

    return valid_features


def _numeric_param_check_range(variable_name, variable_value, range_bottom, range_top):
    """
    Checks if numeric parameter is within given range
    """
    err_msg = "%s must be between %i and %i"

    if variable_value < range_bottom or variable_value > range_top:
        raise ToolkitError, err_msg % (variable_name, range_bottom, range_top)
