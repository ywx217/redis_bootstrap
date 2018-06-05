# coding=utf-8
__author__ = 'ywx217@gmail.com'
from redis_bootstrap.tests import BaseTest


class ContextTest(BaseTest):
	def testContextCreation(self):
		self.assertTrue(self.context)

	def testContextModify(self):
		self.context.hmset('foo', {'k1': 1, 'k2': 1.0, 'k3': 'v3'})
		self.context.flush()
		self.assertEqual(1, int(self.context.hget('foo', 'k1')))
		self.assertEqual(1.0, float(self.context.hget('foo', 'k2')))
		self.assertEqual('v3', self.context.hget('foo', 'k3'))

		self.context.hmset('foo', {'k1': 2, 'k2': 2.0, 'k3': 'v3x'})
		self.context.flush()
		self.assertEqual(2, int(self.context.hget('foo', 'k1')))
		self.assertEqual(2.0, float(self.context.hget('foo', 'k2')))
		self.assertEqual('v3x', self.context.hget('foo', 'k3'))
