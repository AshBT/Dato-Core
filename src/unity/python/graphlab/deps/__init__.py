'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
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
