"""
GraphLab Create can launch an EC2 instance with one line of Python code, then
all GraphLab Create operations are executed on that instance.

Explore the functionalities in detail through the API documentation, or learn
more from the `Running in the Cloud Tutorial. <https://dato.com/learn/notebooks/running_in_the_cloud.html>`_

"""

'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''

from _ec2 import get_credentials, launch_EC2, list_instances, set_credentials, status, terminate_EC2
