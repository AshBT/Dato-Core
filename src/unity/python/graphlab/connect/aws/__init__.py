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
GraphLab Create can launch an EC2 instance with one line of Python code, then
all GraphLab Create operations are executed on that instance.

Explore the functionalities in detail through the API documentation, or learn
more from the `Running in the Cloud Tutorial. <https://dato.com/learn/notebooks/running_in_the_cloud.html>`_

"""
from _ec2 import get_credentials, launch_EC2, list_instances, set_credentials, status, terminate_EC2
