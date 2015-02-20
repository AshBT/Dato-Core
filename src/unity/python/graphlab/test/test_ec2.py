'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''
import ConfigParser
import unittest
import os
import tempfile

from moto import mock_ec2
import boto.ec2

import graphlab
from graphlab.connect.aws import _ec2 as EC2
from graphlab.connect.aws._ec2 import _ec2_factory


class Ec2Test(unittest.TestCase):
    """
    This is the beginning of tests for the EC2 class. VERY basic stuff for now, but we'll use moto and add more tests in
    the near future.
    """

    def setUp(self):
        self.access_key = os.environ.get('AWS_ACCESS_KEY_ID', None)
        self.secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', None)

        if 'AWS_ACCESS_KEY_ID' in os.environ:
            del os.environ['AWS_ACCESS_KEY_ID']
        if 'AWS_SECRET_ACCESS_KEY' in os.environ:
            del os.environ['AWS_SECRET_ACCESS_KEY']

    def tearDown(self):
        if self.access_key: os.environ['AWS_ACCESS_KEY_ID'] = self.access_key
        if self.secret_key: os.environ['AWS_SECRET_ACCESS_KEY'] = self.secret_key

    def test_no_auth_exception(self):
        """
        Verify that no auth shows correct error message
        """

        # Arrange
        instance_type = 'not-a-real-type'
        region = 'us-west-2'

        # Act & Assert
        try:
            ec2 = EC2._Ec2GraphLabServer(instance_type, region, None, None, None, None, '', '')
        except Exception as e:
            self.assertTrue("AWS configuration not found." in e.message, e.message)
        else:
            self.fail('Expected an exception to be raised')

    def test_bad_auth(self):
        """
        Verify that bad auth shows correct error message
        """
        # Arrange
        graphlab.aws.set_credentials('foo', 'bar')

        # Act & Assert
        try:
            ec2 = EC2._Ec2GraphLabServer('not-real-type', 'us-west-2', None, None, None, None, '', '')
        except Exception as e:
            self.assertTrue('EC2 response error. Please verify your AWS credentials' in e.message, e.message)
        else:
            self.fail('Expected an exception to be raised')

    def test_bad_region(self):
        """
        Verify that bad region shows correct error message.
        """
        # Arrange
        graphlab.aws.set_credentials('foo', 'bar')

        # Act & Assert
        try:
            ec2 = EC2._Ec2GraphLabServer('not-real-type', 'not-real-region', None, None, None, None, '', '')
        except Exception as e:
            self.assertTrue('is not a valid region.' in e.message, e.message)
        else:
            self.fail('Expected an exception to be raised')

    def test_config_reading(self):
        # Test retrieving region
        REGION = 'Never Never Land'
        config = ConfigParser.ConfigParser()
        config.add_section(EC2.CONFIG_SECTION)
        config.set(EC2.CONFIG_SECTION, 'region', REGION)
        temp_file = tempfile.NamedTemporaryFile()
        config.write(temp_file)
        temp_file.flush()
        region = EC2._get_region_from_config(temp_file.name)
        self.assertEqual(region, REGION)

        # Test config with our section but not our option
        config = ConfigParser.ConfigParser()
        config.add_section(EC2.CONFIG_SECTION)
        temp_file = tempfile.NamedTemporaryFile()        
        config.write(temp_file)
        temp_file.flush()
        region = EC2._get_region_from_config(temp_file.name)
        self.assertIsNone(region)

        # Test empty file
        temp_file = tempfile.NamedTemporaryFile()
        region = EC2._get_region_from_config(temp_file.name)
        self.assertIsNone(region)

        # Test non-existent file
        region = EC2._get_region_from_config('/foo/bar/noWayThereIsAFileCalledThis')
        self.assertIsNone(region)


    @mock_ec2
    def test_ec2_factory(self):
        os.environ['GRAPHLAB_TEST_AMI_ID'] = 'foo'
        os.environ['GRAPHLAB_TEST_ENGINE_URL'] = 'bar'
        os.environ['GRAPHLAB_TEST_EC2_KEYPAIR'] = 'biz'
        os.environ['AWS_ACCESS_KEY_ID'] = 'fake'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'totally fake'
        os.environ['GRAPHLAB_TEST_HASH_KEY'] = 'embarrassingly_fake' 

        ret = _ec2_factory('m3.xlarge')

        self.assertIsNotNone(ret.instance)
        self.assertIsNotNone(ret.instance.private_dns_name)
        self.assertIsNotNone(ret.instance_id)
        self.assertIsNotNone(ret.public_dns_name)
        self.assertEqual(ret.region, 'us-west-2')

        self.assertEqual(ret.instance.image_id, 'foo')
        self.assertEqual(ret.instance.instance_type, 'm3.xlarge')
        self.assertEqual(ret.instance.key_name, 'biz')

        # Upgrading to moto 0.3.7 broke this test. It worked with moto 0.3.5
        # I've verified this functionality is correct in our 1.0 release.
        #ret.instance.update()   # tags were applied before the last update        
        #self.assertEquals(ret.instance.tags, {u'GraphLab': u'None'})

        self.assertEquals(ret.instance.state, u'running')
        ret.stop()
        ret.instance.update()
        self.assertEquals(ret.instance.state, u'terminated')

    @mock_ec2
    def test_launching_multiple_hosts(self):
        os.environ['GRAPHLAB_TEST_AMI_ID'] = 'foo'
        os.environ['GRAPHLAB_TEST_ENGINE_URL'] = 'bar'
        os.environ['GRAPHLAB_TEST_EC2_KEYPAIR'] = 'biz'
        os.environ['AWS_ACCESS_KEY_ID'] = 'fake'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'totally fake'
        os.environ['GRAPHLAB_TEST_HASH_KEY'] = 'embarrassingly_fake' 

        ret = _ec2_factory('m3.xlarge', num_hosts = 2)
        
        self.assertIsInstance(ret, list)
        self.assertEquals(len(ret), 2)
        self.assertIsInstance(ret[0], EC2._Ec2Instance)
        self.assertIsInstance(ret[1], EC2._Ec2Instance)
