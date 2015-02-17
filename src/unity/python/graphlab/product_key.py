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
from graphlab_util.config import DEFAULT_CONFIG

import logging
import os
import subprocess
import graphlab.sys_util as _sys_util

__LOGGER__ = logging.getLogger(__name__)

__section = 'Product'
__key = 'product_key'

def get_product_key(file=(os.path.join(os.path.expanduser("~"), ".graphlab", "config"))):
    """
    Returns the product key found in file, which by default is ~/.graphlab/config
    or in environment variable GRAPHLAB_PRODUCT_KEY.

    Note: Environment variable takes precedence over config file.

    @param file optional parameter to specify which file to use for configuration (defaults to ~/.graphlab/config)
    @return Product key string, or None if not found.
    @throws KeyError('Missing Product Key')
    """
    PRODUCT_KEY_ENV = 'GRAPHLAB_PRODUCT_KEY'
    if not PRODUCT_KEY_ENV in os.environ:
        import graphlab.connect as _mt
        # see if in ~/.graphlab/config
        config_file = file
        if (os.path.isfile(config_file)):
            try:
                import ConfigParser
                config = ConfigParser.ConfigParser()
                config.read(config_file)
                product_key = config.get(__section, __key)
                if product_key == -1:
                    raise BaseException() # will fall into except block below
                else:
                    # set the product key as an environment variable in this session
                    os.environ[PRODUCT_KEY_ENV] = str(product_key).strip('"\'')
            except:
                msg = "Unable to parse product key out of %s. Make sure it is defined in the [%s] section, with key name: '%s'" % (config_file, __section, __key)
                _mt._get_metric_tracker().track('server_launch.config_parser_error')
                raise KeyError(msg)
        else:
            msg = "No product key found. Please configure your product key by setting the [%s] section with '%s' key in %s or by setting the environment variable GRAPHLAB_PRODUCT_KEY to the product key. If you do not have a product key, please register for one at https://dato.com/register." % (__section, __key, config_file)
            _mt._get_metric_tracker().track('server_launch.product_key_missing')
            raise KeyError(msg)
    return os.environ[PRODUCT_KEY_ENV]

def set_product_key(product_key, file=(os.path.join(os.path.expanduser("~"), ".graphlab", "config"))):
    """
    Sets the product key provided in file, which by default is ~/.graphlab/config
    Overwrites any existing product key in that file.

    Note: Environment variable GRAPHLAB_PRODUCT_KEY takes precedence over the
    config file and is not affected by this function.

    Parameters
    ----------
    product_key : str
        The product key, provided by registration on https://dato.com/register

    file : str, optional
        Specifies which file to use for configuration (defaults to ~/.graphlab/config)
    """
    import graphlab.connect as _mt
    try:
        import ConfigParser
        config = ConfigParser.ConfigParser()
        config.read(file)
        if not(config.has_section(__section)):
            config.add_section(__section)
        config.set(__section, __key, product_key)
        with open(file, 'wb') as config_file:
            config.write(config_file)
        _mt._get_metric_tracker().track('set_product_key.succeeded')
    except:
        _mt._get_metric_tracker().track('set_product_key.config_parser_error')
        raise

def is_product_key_valid(product_key, config=DEFAULT_CONFIG):
    """
    Validate the product key passed in by launching local server process.

    @param key to validate
    @return True if validates correctly, False otherwise

    Raises a RuntimeError if the unity_server binary cannot be executed.
    """
    try:
        cmd = "%s --help" % (config.server_bin)
        subprocess.check_call(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=_sys_util.make_unity_server_env(), shell=True)
    except:
        raise RuntimeError("Cannot execute unity_server binary")

    try:
        cmd = "%s --check_product_key_only --product_key='%s'" % (config.server_bin, product_key)
        subprocess.check_output(cmd, env=_sys_util.make_unity_server_env(), shell=True)
        return True
    except Exception as e:
        __LOGGER__.debug(config)
        __LOGGER__.debug(e)
        pass

    return False
