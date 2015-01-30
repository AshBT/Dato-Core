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
from distutils.version import StrictVersion
import logging

def __get_version(version): 
    if 'dev' in str(version):
        version = version[:version.find('.dev')]

    return StrictVersion(version)


HAS_PANDAS = True
PANDAS_MIN_VERSION = '0.13.0'
try:
    import pandas
    if __get_version(pandas.__version__) < StrictVersion(PANDAS_MIN_VERSION):
        HAS_PANDAS = False
        logging.warn(('Pandas version %s is not supported. Minimum required version: %s. '
                      'Pandas support will be disabled.')
                      % (pandas.__version__, PANDAS_MIN_VERSION) )
except:
    HAS_PANDAS = False
    import pandas_mock as pandas


HAS_NUMPY = True
NUMPY_MIN_VERSION = '1.8.0'
try:
    import numpy

    if __get_version(numpy.__version__) < StrictVersion(NUMPY_MIN_VERSION):
        HAS_NUMPY = False
        logging.warn(('Numpy version %s is not supported. Minimum required version: %s. '
                      'Numpy support will be disabled.')
                      % (numpy.__version__, NUMPY_MIN_VERSION) )
except:
    HAS_NUMPY = False
    import numpy_mock as numpy
