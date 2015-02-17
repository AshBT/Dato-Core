"""
Builtin aggregators for SFrame groupby operator.
"""

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

def SUM(src_column):
  """
  Builtin sum aggregator for groupby.

  Example: Get the sum of the rating column for each user. If
  src_column is of array type, if array's do not match in length a NoneType is
  returned in the destination column.

  >>> sf.groupby("user",
                 {'rating_sum':gl.aggregate.SUM('rating')})

  """
  return ("__builtin__sum__", [src_column])

def ARGMAX(agg_column,out_column):
  """
  Builtin arg maximum aggregator for groupby

  Example: Get the movie with maximum rating per user.

  >>> sf.groupby("user",
                 {'best_movie':gl.aggregate.ARGMAX('rating','movie')})
  """
  return ("__builtin__argmax__",[agg_column,out_column])

def ARGMIN(agg_column,out_column):
  """
  Builtin arg minimum aggregator for groupby

  Example: Get the movie with minimum rating per user.

  >>> sf.groupby("user",
                 {'best_movie':gl.aggregate.ARGMIN('rating','movie')})

  """
  return ("__builtin__argmin__",[agg_column,out_column])

def MAX(src_column):
  """
  Builtin maximum aggregator for groupby

  Example: Get the maximum rating of each user.

  >>> sf.groupby("user",
                 {'rating_max':gl.aggregate.MAX('rating')})

  """
  return ("__builtin__max__", [src_column])

def MIN(src_column):
  """
  Builtin minimum aggregator for groupby

  Example: Get the minimum rating of each user.

  >>> sf.groupby("user",
                 {'rating_min':gl.aggregate.MIN('rating')})

  """
  return ("__builtin__min__", [src_column])


def COUNT(*args):
  """
  Builtin count aggregator for groupby

  Example: Get the number of occurrences of each user.

  >>> sf.groupby("user",
                 {'count':gl.aggregate.COUNT()})

  """
  # arguments if any are ignored
  return ("__builtin__count__", [""])



def AVG(src_column):
  """
  Builtin average aggregator for groupby. Synonym for gl.aggregate.MEAN. If
  src_column is of array type, and if array's do not match in length a NoneType is
  returned in the destination column.

  Example: Get the average rating of each user.

  >>> sf.groupby("user",
                 {'rating_avg':gl.aggregate.AVG('rating')})

  """
  return ("__builtin__avg__", [src_column])


def MEAN(src_column):
  """
  Builtin average aggregator for groupby. Synonym for gl.aggregate.AVG. If
  src_column is of array type, and if array's do not match in length a NoneType is
  returned in the destination column.


  Example: Get the average rating of each user.

  >>> sf.groupby("user",
                 {'rating_mean':gl.aggregate.MEAN('rating')})

  """
  return ("__builtin__avg__", [src_column])

def VAR(src_column):
  """
  Builtin variance aggregator for groupby. Synonym for gl.aggregate.VARIANCE

  Example: Get the rating variance of each user.

  >>> sf.groupby("user",
                 {'rating_var':gl.aggregate.VAR('rating')})

  """
  return ("__builtin__var__", [src_column])


def VARIANCE(src_column):
  """
  Builtin variance aggregator for groupby. Synonym for gl.aggregate.VAR

  Example: Get the rating variance of each user.

  >>> sf.groupby("user",
                 {'rating_var':gl.aggregate.VARIANCE('rating')})

  """
  return ("__builtin__var__", [src_column])


def STD(src_column):
  """
  Builtin standard deviation aggregator for groupby. Synonym for gl.aggregate.STDV

  Example: Get the rating standard deviation of each user.

  >>> sf.groupby("user",
                 {'rating_std':gl.aggregate.STD('rating')})

  """
  return ("__builtin__stdv__", [src_column])


def STDV(src_column):
  """
  Builtin standard deviation aggregator for groupby. Synonym for gl.aggregate.STD

  Example: Get the rating standard deviation of each user.

  >>> sf.groupby("user",
                 {'rating_stdv':gl.aggregate.STDV('rating')})

  """
  return ("__builtin__stdv__", [src_column])


def SELECT_ONE(src_column):
  """
  Builtin aggregator for groupby which selects one row in the group.

  Example: Get one rating row from a user.

  >>> sf.groupby("user",
                 {'rating':gl.aggregate.SELECT_ONE('rating')})

  If multiple columns are selected, they are guaranteed to come from the
  same row. for instance:
  >>> sf.groupby("user",
                 {'rating':gl.aggregate.SELECT_ONE('rating')},
                 {'item':gl.aggregate.SELECT_ONE('item')})

  The selected 'rating' and 'item' value for each user will come from the
  same row in the SFrame.

  """
  return ("__builtin__select_one__", [src_column])

def CONCAT(src_column, dict_value_column = None):
  """
  Builtin aggregator that combines values from one or two columns in one group
  into either a dictionary value, list value or array value

  For example, to combine values from two columns that belong to one group into
  one dictionary value:

  >>> sf.groupby(["document"],
       {"word_count": gl.aggregate.CONCAT("word", "count")})

  To combine values from one column that belong to one group into a list value:

  >>> sf.groupby(["user"],
       {"friends": gl.aggregate.CONCAT("friend")})

  """
  if dict_value_column == None:
    return ("__builtin__concat__list__", [src_column])
  else:
    return ("__builtin__concat__dict__", [src_column, dict_value_column])


def QUANTILE(src_column, *args):
    """
    Builtin approximate quantile aggregator for groupby.
    Accepts as an argument, one or more of a list of quantiles to query.
    For instance:

    To extract the median

    >>> sf.groupby("user", {'rating_quantiles':gl.aggregate.QUANTILE('rating', 0.5)})

    To extract a few quantiles

    >>> sf.groupby("user", {'rating_quantiles':gl.aggregate.QUANTILE('rating', [0.25,0.5,0.75])})

    Or equivalently

    >>> sf.groupby("user", {'rating_quantiles':gl.aggregate.QUANTILE('rating', 0.25,0.5,0.75)})

    The returned quantiles are guaranteed to have 0.5% accuracy. That is to say,
    if the requested quantile is 0.50, the resultant quantile value may be
    between 0.495 and 0.505 of the true quantile.
    """
    if len(args) == 1:
        quantiles = args[0]
    else:
        quantiles = list(args)

    if not hasattr(quantiles, '__iter__'):
        quantiles = [quantiles]
    query = ",".join([str(i) for i in quantiles])
    return ("__builtin__quantile__[" + query + "]", [src_column])
