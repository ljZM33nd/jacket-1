__author__ = 'wangfeng'
"""
:mod:`aws` -- Hybrid cloud support for Amazon Web Service using libclou sdk.
"""

import driver
import driver_agentless

#VCloudDriver = driver.VCloudDriver
AwsEc2Driver = driver.AwsEc2Driver
AwsAgentlessDriver = driver_agentless.AwsAgentlessDriver
