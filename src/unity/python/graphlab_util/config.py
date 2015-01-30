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
import os
import time
import logging

class GraphLabConfig:

    __slots__ = ['graphlab_server', 'server_addr', 'server_bin', 'log_dir', 'unity_metric',
                 'mode', 'version', 'librato_user', 'librato_token', 'mixpanel_user',
                 'log_rotation_interval','log_rotation_truncate', 'metrics_url']

    def __init__(self, server_addr=None):
        if not server_addr:
            server_addr = 'default'
        self.server_addr = server_addr
        gl_root = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'graphlab'))
        self.log_rotation_interval = 86400
        self.log_rotation_truncate = 8
        if "GRAPHLAB_UNITY" in os.environ:
            self.server_bin = os.environ["GRAPHLAB_UNITY"]
        elif os.path.exists(os.path.join(gl_root, "unity_server")):
            self.server_bin = os.path.join(gl_root, "unity_server")

        if "GRAPHLAB_LOG_ROTATION_INTERVAL" in os.environ:
            tmp = os.environ["GRAPHLAB_LOG_ROTATION_INTERVAL"]
            try:
                self.log_rotation_interval = int(tmp)
            except:
                logging.getLogger(__name__).warning("GRAPHLAB_LOG_ROTATION_INTERVAL must be an integral value")

        if "GRAPHLAB_LOG_ROTATION_TRUNCATE" in os.environ:
            tmp = os.environ["GRAPHLAB_LOG_ROTATION_TRUNCATE"]
            try:
                self.log_rotation_truncate = int(tmp)
            except:
                logging.getLogger(__name__).warning("GRAPHLAB_LOG_ROTATION_TRUNCATE must be an integral value")

        if "GRAPHLAB_LOG_PATH" in os.environ:
            log_dir = os.environ["GRAPHLAB_LOG_PATH"]
        else:
            log_dir = "/tmp"

        self.log_dir = log_dir
        ts = str(int(time.time()))
        self.unity_metric = os.path.join(log_dir, 'graphlab_metric_' + str(ts) + '.log')

        # Import our unity conf file if it exists, and get the mode from it
        try:
          import graphlab_env
        except ImportError:
          self.graphlab_server = 'http://pws-billing-stage.herokuapp.com'
          self.mode = 'UNIT'
          self.version = '0.1.desktop'
          self.librato_user = ''
          self.librato_token = ''
          self.mixpanel_user = ''
          self.metrics_url = ''
        else:
          if graphlab_env.mode in ['UNIT', 'DEV', 'QA', 'PROD']:
            self.mode = graphlab_env.mode
          else:
            self.mode = 'PROD'

          self.version = graphlab_env.version
          self.librato_user = graphlab_env.librato_user
          self.librato_token = graphlab_env.librato_token
          self.mixpanel_user = graphlab_env.mixpanel_user
          self.graphlab_server = graphlab_env.graphlab_server
          self.metrics_url = graphlab_env.metrics_url

          # NOTE: Remember to update slots if you are adding any config parameters to this file.

    def get_unity_log(self):
        ts = str(int(time.time()))
        return os.path.join(self.log_dir, 'graphlab_server_' + str(ts) + '.log')

DEFAULT_CONFIG = GraphLabConfig()
