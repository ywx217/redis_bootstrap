# coding=utf-8
__author__ = 'ywx217@gmail.com'
import unittest
from redis_bootstrap.context import *


class BaseTest(unittest.TestCase):

	def __init__(self, methodName='runTest'):
		super(BaseTest, self).__init__(methodName)
		self.context = None

	def setUp(self):
		super(BaseTest, self).setUp()
		redis_manager = RedisManager('127.0.0.1', 6379, '0')
		self.context = redis_manager.create_context()
		redis_manager.get_redis().flushdb()

