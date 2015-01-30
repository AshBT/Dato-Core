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
"""@package graphlab.connect.aws.ec2

This module makes it easy to have a GraphLab server running on an EC2 instance.
"""

import ConfigParser
import json
import logging
import os
import time
import urllib2

import boto.ec2
from boto.exception import EC2ResponseError

import graphlab.product_key
import graphlab.connect.server as glserver
import graphlab.connect.main as glconnect
from graphlab.connect.main import __catch_and_log__

from graphlab.cython.cy_ipc import PyCommClient as Client
from graphlab.cython.cy_ipc import get_public_secret_key_pair

from graphlab_util.config import DEFAULT_CONFIG as config
from graphlab.connect import _get_metric_tracker


CONFIG_SECTION = 'AWS'
DEFAULT_CIDR_RULE = '0.0.0.0/0'  # Open to the entire internet.
DEFAULT_INSTANCE_TYPE = 'm3.xlarge'
JSON_BLOB_PATH_FORMAT = "/gl_api/api/v1/cloud/aws/instances/%s?client_version=%s&region=%s&product_key=%s"
GRAPHLAB_NAME = 'GraphLab'
VALID_REGIONS = [r.name for r in boto.ec2.get_regions('ec2')]

# Port 9000 & 9001 - used when running in server mode: only server running with external client.
# Port 9002 & 9003 - used for metrics (deprecated).
# Port 9004 - Commander daemon for jobs/tasks.
# Port 9005 & 9006 - used by predictive service (webservice & reddis).
# Port 9007 - Worker daemon for jobs/tasks.
GRAPHLAB_PORT_MIN_NUM = 9000
GRAPHLAB_PORT_MAX_NUM = 9007
ADDITIONAL_REDIS_PORT = 19006

# Swallow all boto logging, except critical issues
logging.getLogger('boto').setLevel(logging.CRITICAL)

__LOGGER__ = logging.getLogger(__name__)

class LicenseValidationException(Exception):
    pass

class _Ec2Instance:
    def __init__(self, public_dns_name, instance_id, instance, region):
        self.public_dns_name = public_dns_name
        self.instance_id = instance_id
        self.instance = instance
        self.region = region

    def stop(self):
        _stop_instances([self.instance_id], self.region)


class _Ec2GraphLabServer(glserver.RemoteServer):
    def __init__(self, instance_type, region, CIDR_rule, auth_token, security_group_name, tags,
                 public_key, secret_key):
        self.logger = __LOGGER__
        self.auth_token = auth_token

        _get_metric_tracker().track(('ec2.instance_type.%s' % instance_type), value=1)
        _get_metric_tracker().track(('ec2.region.%s' % region), value=1)

        # Configure user data that will be sent to EC2 instance.
        user_data = {}
        if self.auth_token is not None:
            user_data['auth_token'] = self.auth_token
        else:
            user_data['auth_token'] = ''
        if secret_key != '':
            user_data['secret_key'] = secret_key

        # Launch the EC2 instance.
        ec2_instance = _ec2_factory(instance_type=instance_type, region=region, CIDR_rule=CIDR_rule,
                                    security_group_name=security_group_name, tags=tags,
                                    user_data = user_data)
        self.instance = ec2_instance.instance
        self.public_dns_name = self.instance.public_dns_name
        self.region = ec2_instance.region

        # If auth_token is not set by the input argument, the EC2 instance
        # will use its instance id as the default auth_token, so we reset
        # self.auth_token after the instance is running.
        if not self.auth_token:
            self.auth_token = self.instance.id

        if self.instance.state == 'running':
            self.instance_id = self.instance.id
            server_addr = 'tcp://' + self.instance.ip_address + ':' + str(GRAPHLAB_PORT_MIN_NUM)
            self.logger.debug("EC2 instance is now running, attempting to connect to engine.")
            super(self.__class__, self).__init__(server_addr, auth_token=self.auth_token,
                                                 public_key=public_key)
        else:   # Error case
            self.logger.error('Could not startup EC2 instance.')
            if(hasattr(self.instance, 'id')):
                self.stop()
            return


    def get_auth_token(self):
        return self.auth_token

    def stop(self):
        _stop_instances([self.instance_id], self.region)


def _get_region_from_config(config_path = (os.path.join(os.path.expanduser("~"), ".graphlab", "config"))):
    result = None
    if (os.path.isfile(config_path)):
        config = ConfigParser.ConfigParser()
        config.read(config_path)
        if(config.has_section(CONFIG_SECTION) and config.has_option(CONFIG_SECTION, 'region')):
            result = config.get(CONFIG_SECTION, 'region')
    return result


def get_credentials():
    """
    Returns the values stored in the AWS credential environment variables.
    Returns the value stored in the AWS_ACCESS_KEY_ID environment variable and
    the value stored in the AWS_SECRET_ACCESS_KEY environment variable.

    Returns
    -------
    out : tuple [string]
        The first string of the tuple is the value of the AWS_ACCESS_KEY_ID
        environment variable. The second string of the tuple is the value of the
        AWS_SECRET_ACCESS_KEY environment variable.

    See Also
    --------
    set_credentials

    Examples
    --------
    >>> graphlab.aws.get_credentials()
    ('RBZH792CTQPP7T435BGQ', '7x2hMqplWsLpU/qQCN6xAPKcmWo46TlPJXYTvKcv')
    """

    if (not 'AWS_ACCESS_KEY_ID' in os.environ):
        raise KeyError('No access key found. Please set the environment variable AWS_ACCESS_KEY_ID, or using graphlab.aws.set_credentials()')
    if (not 'AWS_SECRET_ACCESS_KEY' in os.environ):
        raise KeyError('No secret key found. Please set the environment variable AWS_SECRET_ACCESS_KEY, or using graphlab.aws.set_credentials()')
    return (os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])


def launch_EC2(instance_type=DEFAULT_INSTANCE_TYPE, region=None, CIDR_rule=None, auth_token=None,
               security_group=None, tags=None, use_secure_connection=True):
    """
    Launches an EC2 instance. All subsequent GraphLab operations will be
    executed on that host. Prior to calling this function, AWS credentials must
    be set as environment variables (see
    :py:func:`~graphlab.aws.set_credentials`).

    Parameters
    ----------
    instance_type : string, optional
        The EC2 instance type to launch. We support `all instance types
        <http://aws.amazon.com/ec2/instance-types/#Instance_Types>`_ that are not
        micros, smalls or mediums.

    region : string, optional
        The `AWS region
        <http://docs.aws.amazon.com/general/latest/gr/rande.html#ec2_region>`_
        in which to launch your instance. Default is 'us-west-2'.

    CIDR_rule : string or list[string], optional
        The Classless Inter-Domain Routing rule(s) to use for the instance.
        Useful for restricting the IP Address Range for a client. Default is no
        restriction. If you specify CIDR_rule(s), you must also specify a
        security group to use.

    auth_token : string, optional
        The Authentication Token to be used by the instance. By default a
        randomly generated token is used.

    security_group : string, optional
        The name of the security group for the EC2 instance to use.

    tags : dict, optional
        A dictionary containing the name/value tag pairs to be assigned to the
        instance. If you want to create only a tag name, the value for that tag
        should be the empty string (i.e. ''). In addition to these specified
        tags, a 'GraphLab' tag will also be assigned.

    use_secure_connection : bool, optional
       Determine whether the communication, between your computer and the EC2
       instance, should be encrypted.

    See Also
    --------
    terminate_EC2

    Examples
    --------
    >>> # Launch a general purpose xlarge host in Oregon.
    >>> graphlab.aws.launch_EC2()

    >>> # Launch an xlarge compute optimized host in the US East Region.
    >>> graphlab.aws.launch_EC2(instance_type='c3.xlarge', region='us-east-1')
    """

    # Check existing connection
    if glconnect.is_connected():
        __LOGGER__.error('You have GraphLab objects instantiated on the current server. If you want to \n'
                           'launch a new server, you must first stop the current server connection by running: \n'
                           '\'graphlab.connect.main.stop()\'. Be warned: you will lose these instantiated objects.')
        return

    assert not glconnect.is_connected()

    (server_public_key, server_secret_key, client_public_key, client_secret_key) = ("", "", "", "")
    if use_secure_connection:
        (server_public_key, server_secret_key) = get_public_secret_key_pair()
        (client_public_key, client_secret_key) = get_public_secret_key_pair()
        __LOGGER__.debug('Launching server with public key: %s' % server_public_key)

    try:
        server = _Ec2GraphLabServer(instance_type, region, CIDR_rule, auth_token, security_group, tags,
                                server_public_key, server_secret_key)

        try:
            # Once the EC2 instance starts up, the client still need to wait for graphlab engine to start up.
            # So allow for a larger than normal number of ping failures. This is especially important when launching
            # in other AWS regions (since longer latency and longer download from S3 for engine binary)
            server.start(num_tolerable_ping_failures=120)
    
        except Exception as e:
            __LOGGER__.error("Unable to successfully connect to GraphLab Server on EC2 instance: '%s'."
                         " Please check AWS Console to make sure any EC2 instances launched have"
                         " been terminated." % e)
            server.stop()
            raise e
    
    except LicenseValidationException as e:
        # catch exception and print license check hint message here instead of raise
        __LOGGER__.info(e)
        return 
    # Create the client
    num_tolerable_ping_failures = 3
    client = Client([], server.get_server_addr(), num_tolerable_ping_failures, public_key=client_public_key,
                secret_key=client_secret_key, server_public_key=server_public_key)
    auth_token = server.get_auth_token()
    if auth_token:
        client.add_auth_method_token(auth_token)
    client.start()
    glconnect._assign_server_and_client(server, client)



def set_credentials(access_key_id, secret_access_key):
    """
    Sets the AWS credential environment variables.

    Helper function to set the following environment variables: AWS_ACCESS_KEY_ID and
    AWS_SECRET_ACCESS_KEY. You are also free to set these environment variables directly.

    Parameters
    ----------
    access_key_id : str
        Value for the AWS_ACCESS_KEY_ID environment variable.

    secret_access_key : str
        Value for the AWS_SECRET_ACCESS_KEY environment variable.

    See Also
    --------
    get_credentials

    Examples
    --------
    >>> graphlab.aws.set_credentials('RBZH792CTQPP7T435BGQ', '7x2hMqplWsLpU/qQCN6xAPKcmWo46TlPJXYTvKcv')

    """
    os.environ['AWS_ACCESS_KEY_ID'] = str(access_key_id)
    os.environ['AWS_SECRET_ACCESS_KEY'] = str(secret_access_key)


@__catch_and_log__
def list_instances(region='us-west-2'):
    """
    Instance IDs for all active GraphLab EC2 hosts. Returns instance IDs for all
    active GraphLab EC2 hosts (for the specified region), not just the instance
    ID associated with the current python session.

    Parameters
    ----------
    region : string, optional
        The `AWS region
        <http://docs.aws.amazon.com/general/latest/gr/rande.html#ec2_region>`_
        in which to list instances. Default is 'us-west-2'.

    Returns
    -------
    out : list[string]
        A list of AWS instance IDs corresponding to active instances in the
        specified region.

    Examples
    --------
    .. sourcecode:: python

        # No instances running in 'us-west-2'.
        >>> graphlab.aws.list_instances()
        []

        # Two insances running in 'us-east-1'.
        >>> graphlab.aws.list_instances(region = 'us-east-1')
        [u'i-8df559a7', u'i-ed2aa9c1']
    """

    __LOGGER__.info("Listing instances in %s." % region)
    aws_connection = boto.ec2.connect_to_region(region)
    recent_ids = [i.res_id for i in aws_connection.get_all_tags(filters={'tag:GraphLab': ''})]
    active_instnaces = aws_connection.get_all_instance_status(instance_ids=recent_ids)
    return [i.id for i in active_instnaces]


@__catch_and_log__
def status():
    """
    Returns the status of any running EC2 instance associated with the current
    python session.

    Examples
    --------
    >>> # With an EC2 instance running.
    >>> graphlab.aws.status()
    u'running'

    >>> # Without an EC2 instance running.
    >>> graphlab.aws.status()
    >>> 'No instance running.'
    """

    server = glconnect.get_server()
    if server is None or not isinstance(server, _Ec2GraphLabServer):
        return 'No instance running.'
    aws_connection = boto.ec2.connect_to_region(server.region)
    status = aws_connection.get_all_instance_status(instance_ids=[server.instance_id])
    __LOGGER__.debug("EC2 status returned: %s" % status)
    if(not(len(status) == 1)):
        __LOGGER__.error('Invalid response from EC2. Unable to determine EC2 status. Please consult '
                         'the AWS EC2 Console.')

    return status[0].state_name


def terminate_EC2():
    """
    Terminates the EC2 instance associated with the current python session.

    Examples
    --------
    >>> graphlab.aws.terminate_EC2()
    """
    server = glconnect.get_server()
    if server is not None and isinstance(server, _Ec2GraphLabServer):
        __LOGGER__.debug("Stopping EC2 instance.")
        glconnect.stop()
    else:
        __LOGGER__.info("No EC2 instance to stop.")


def _stop_instances(instance_id_list, region, credentials = {}):
    __LOGGER__.info("Terminating EC2 host(s) %s in %s" % (instance_id_list, region))

    # Hopefully we will not need this string
    ERROR_MSG = 'Invalid response from EC2. Unable to terminate host(s). Please terminate using the ' \
        'AWS EC2 Console.'

    try:
        # Shut the host(s) down.
        conn = boto.ec2.connect_to_region(region, **credentials)
        response = conn.terminate_instances(instance_ids = instance_id_list)

        # Sanity check response.
        if(not(len(response) == len(instance_id_list))):
            raise Exception(ERROR_MSG)
        for r in response:
            status = r.update()
            if status not in ['shutting-down', 'terminated']:
                raise Exception(ERROR_MSG)
    except:
        raise Exception(ERROR_MSG)


def _ec2_factory(instance_type, region=None, availability_zone=None,
                 CIDR_rule=None, security_group_name=None, tags=None,
                 user_data = {}, credentials = {}, ami_service_parameters = {}, num_hosts = 1):
    '''
    This function does everything necessary to bring up EC2 host(s): create a security group (if
    nessary), determine arguments to start up the EC2 instance (i.e. AMI id and user data),
    actually start up the EC2 instance, wait for it, and applies AWS tags.
    '''
    # Get and validate the product key
    product_key = graphlab.product_key.get_product_key()
    if (not graphlab.product_key.is_product_key_valid(product_key)):
        raise LicenseValidationException("Product Key validation failed, please confirm your product key is correct.\
 If you believe this key to be valid, please contact support@dato.com")
    
    # Set default values for parameters.
    if(region is None):
        region = _get_region_from_config()
        if(region is None):
            region = 'us-west-2'
        else:
            __LOGGER__.info('Read region from config file.')

    if (region not in VALID_REGIONS):
        raise Exception("%s is not a valid region." % region)

    # If a CIDR rule is specified, a security group must also be specified.
    if CIDR_rule is not None and security_group is None:
        __LOGGER__.error('If you specify a CIDR rule, you must also specify an existing security group.')
        return
    # If no security group is specified (and no CIDR rule), use a default value for the security group name.
    elif security_group_name is None:
        security_group_name = GRAPHLAB_NAME
    if CIDR_rule is None:
        CIDR_rule = DEFAULT_CIDR_RULE

    # Setup/get the security group.
    try:
        conn = boto.ec2.connect_to_region(region, **credentials)
        # Does the security group already exist?
        security_group = None
        for sg in conn.get_all_security_groups():
            if(security_group_name == sg.name):
                security_group = sg
                break   # found it
    except boto.exception.EC2ResponseError as e:
        # This is the first AWS contact, so the most likely cause is env vars aren't set.
        __LOGGER__.debug('EC2 Response Error: %s' % e)
        raise Exception('EC2 response error. Please verify your AWS credentials. To configure your '
                'AWS credentials use graphlab.aws.set_credentials or set the '
                'AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables'
                'Exception: %s' % e)
    except boto.exception.NoAuthHandlerFound as e:
        raise Exception('AWS configuration not found. Please configure your AWS credentials '
                'using graphlab.aws.set_credentials or by setting AWS_ACCESS_KEY_ID '
                'and AWS_SECRET_ACCESS_KEY environment variables')

    # If security group doesn't exist, create it.
    if security_group is None:
        security_group = conn.create_security_group(security_group_name,
                'Only open ports needed for GraphLab Server')

    # Configure the security group
    try:
        security_group.authorize('tcp', GRAPHLAB_PORT_MIN_NUM, GRAPHLAB_PORT_MAX_NUM, CIDR_rule)
        security_group.authorize('tcp', 443, 443, CIDR_rule)
        security_group.authorize('tcp', 80, 80, CIDR_rule)
        security_group.authorize('tcp', ADDITIONAL_REDIS_PORT, ADDITIONAL_REDIS_PORT, CIDR_rule)
    except EC2ResponseError as e:
        if not "<Code>InvalidPermission.Duplicate</Code>" in str(e):
            raise e

    # Determine AMI id, as well as engine, os URL, and hash value
    if ('GRAPHLAB_TEST_AMI_ID' in os.environ and 'GRAPHLAB_TEST_ENGINE_URL' in os.environ and 'GRAPHLAB_TEST_HASH_KEY' in os.environ):
        # unit-test mode, don't involve webservice to retrieve AMI, instead use environment variables
        ami_id = os.environ['GRAPHLAB_TEST_AMI_ID']
        engine_url = os.environ['GRAPHLAB_TEST_ENGINE_URL']
        __LOGGER__.info("UNIT mode, using AMI: '%s' and engine url: '%s' when launching EC2 instance." % (ami_id, engine_url))
        json_blob = json.loads('{}')
        json_blob['ami_id'] = ami_id
        json_blob['engine_url'] = engine_url
        json_blob['hash_key'] = os.environ['GRAPHLAB_TEST_HASH_KEY']
    else:
        # Get the info to start a EC2 from the GraphLab Server
        json_blob_path = JSON_BLOB_PATH_FORMAT % (instance_type, config.version, region, product_key)
        for (param_name, param_value) in ami_service_parameters.items():
            json_blob_path += "&%s=%s" % (str(param_name), str(param_value))
        json_blob_url = config.graphlab_server + json_blob_path

        try:
            # set specific timeout for this web service request, lots of time spent in SSL negotiation
            graphlab_server_response = urllib2.urlopen(json_blob_url, timeout=10)
            json_blob = json.loads(graphlab_server_response.read())
        except:
            raise Exception('Unable to successfully retrieve correct EC2 image to launch for this '
                    'version. This could be a temporary problem. Please try again in a few '
                    'minutes. If the problem persists please contact support@dato.com')
        __LOGGER__.debug("web service return: %s" % json_blob)
        
        if json_blob.get('error'):
            raise LicenseValidationException(json_blob.get('error'))

    if 'ami_id' not in json_blob or json_blob['ami_id'] is None:
        raise Exception("Unable to successfully retrieve correct EC2 image to launch. Please try "
                "again later or contact support@dato.com. Error received:'%s'"
                % json_blob.get('message'))
    ami_id = json_blob['ami_id']

    # Add json_blob to user_data and set the product key and hash key
    user_data.update(json_blob)
    user_data['product_key'] = product_key

    user_data['hash_key'] = json_blob.get('hash_key', 'NO_HASH_VALUE')
    
    # Check for empty os_url 
    if user_data.get('os_url') is None or len(user_data.get('os_url')) == 0:
        user_data['os_url'] = 'NO_OS_URL'


    # Check for testing override of os_url param.
    if (config.mode != 'PROD' and 'GRAPHLAB_TEST_OS_URL' in os.environ):
        user_data['os_url'] = os.environ['GRAPHLAB_TEST_OS_URL']

    run_instances_args =  {
            'security_group_ids' : [ security_group_name ],
            'user_data' : json.dumps(user_data),
            'instance_type' : instance_type,
            'placement' : availability_zone
            }

    if num_hosts != 1:
        run_instances_args['min_count'] = num_hosts
        run_instances_args['max_count'] = num_hosts

    # If this isn't prod, may not to set SSH key for debugging.
    if 'GRAPHLAB_TEST_EC2_KEYPAIR' in os.environ:
        keypair = os.environ['GRAPHLAB_TEST_EC2_KEYPAIR']
        __LOGGER__.info("Using keypair: '%s' when launching EC2 instance" % (keypair))
        run_instances_args['key_name'] = keypair

    # Actually launch the EC2 instance(s) and wait for them to start running.
    instances = None
    try:
        response = conn.run_instances(ami_id, **run_instances_args)
        instances = response.instances
        if(len(response.instances) != num_hosts):
            raise Exception

        # Report
        for i in instances:
            __LOGGER__.info("Launching an %s instance in the %s availability zone, with id: %s."
                    " You will be responsible for the cost of this instance."
                    % (i.instance_type, i.placement, i.id))

        # Wait for all host(s) to say they're done starting up.
        while True:
            try:
                for i in instances:
                    # Rarely an instance can a reach temp state before going into pending. We check for
                    # 'running' right away to make unit tests work.
                    while not i.update() in ['pending', 'running', 'failed']:
                        time.sleep(1)
                    while i.update() == 'pending':
                        time.sleep(1)
                    if i.update() == 'failed':
                        raise RuntimeError("Instance %s startup failed" % i.id)
                break
            except EC2ResponseError as e:
                # EC2 is eventual consistence so sometimes it complains that it
                # cannot find the instance, in that case, we will retry
                __LOGGER__.debug("Ignoring EC2ResponseError: %s" % e.message)

    except Exception as e:
        if instances:
            _stop_instances([i.id for i in instances] , self.region)
        raise Exception("Unable to launch EC2 instance: '%s'. Please check AWS Console to make "
                " sure any EC2 instances launched have been terminated." % e)

    # Add tags to this instance(s).
    if(tags is None):
        tags = {}
    tags[GRAPHLAB_NAME] = ''
    for i in instances:
        conn.create_tags(i.id, tags)

    results = []
    for i in instances:
        results.append(_Ec2Instance(i.public_dns_name, i.id, i, region))

    if num_hosts == 1:
        # for backward compatibility
        return results[0]
    return results


def _get_ec2_instances(ec2_id_list, region, aws_credentials = {}):
    conn = boto.ec2.connect_to_region(region, **aws_credentials)
    response = conn.get_all_instances(instance_ids = ec2_id_list)

    results = []
    for reservation in response:
        results += reservation.instances

    return results
