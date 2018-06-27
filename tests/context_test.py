# coding=utf-8
__author__ = 'ywx217@gmail.com'

from redis import RedisError, WatchError
from redis_bootstrap.tests import BaseTest


class ContextTest(BaseTest):
	def testContextCreation(self):
		self.assertTrue(self.context)

	def testContextModify(self):
		self.context.hmset('foo', {'k1': 1, 'k2': 1.0, 'k3': 'v3'})
		self.assertEqual(1, int(self.context.hget('foo', 'k1')))
		self.assertEqual(1.0, float(self.context.hget('foo', 'k2')))
		self.assertEqual('v3', self.context.hget('foo', 'k3'))

		self.context.hmset('foo', {'k1': 2, 'k2': 2.0, 'k3': 'v3x'})
		self.assertEqual(2, int(self.context.hget('foo', 'k1')))
		self.assertEqual(2.0, float(self.context.hget('foo', 'k2')))
		self.assertEqual('v3x', self.context.hget('foo', 'k3'))

	def testContextWatchFail(self):
		self.context.hmset('foo', {'k': 'v'})

		self.context.watch('foo')
		self.context.hmset('foo', {'k': 'v2'})

		def _f():
			with self.context:
				self.context.hmset('foo', {'k': 'v3'})

		self.assertRaises(WatchError, _f)

	def testContextWatchAfterStart(self):
		def _f():
			with self.context:
				self.context.watch('foo')

		self.assertRaises(RedisError, _f)

	def testContextMultiEnter(self):
		def _f1():
			with self.context:
				with self.context:
					pass

		self.assertRaises(RedisError, _f1)
		with self.context:
			pass

		def _f2():
			with self.context:
				self.context.flush()

		self.assertRaises(RedisError, _f2)
		with self.context:
			pass

		self.context.begin()
		self.context.reset()
		self.context.begin()
		self.context.flush()

	def _modInContext(self):
		with self.context:
			# MULTI
			# outer HSET
			self.context._redis.hset('foo', '1', '1')
			# HSET
			self.context.hset('foo', '1', '1')
			# EXEC

	def _noModInContext(self):
		with self.context:
			self.context.hset('foo', '1', '1')

	def testWatchError(self):
		self.context.watch('foo')
		self.assertRaises(WatchError, self._modInContext)

	def testDoubleMulti(self):
		self.context.watch('foo')

		with self.context:
			self.context.hset('another', '1', '1')

		# all UNWATCHed
		with self.context:
			self.context._redis.hset('foo', '1', '1')
			self.context.hset('foo', '1', '1')

		# re-WATCH
		self.context.watch('foo')
		self.assertRaises(WatchError, self._modInContext)

	def testModifyBeforeBeginTransaction(self):
		self.context.watch('foo')
		self.context._redis.hset('foo', '1', '1')

		self.assertRaises(WatchError, self._noModInContext)

	def testDoubleContextWatch(self):
		another_context = self.newContext()
		self.context.watch('foo')
		another_context.watch('foo')

		self.context._redis.hset('foo', '1', '1')

		self.assertRaises(WatchError, self._noModInContext)

		def _watchError():
			with another_context:
				another_context.hset('foo', '1', '1')

		self.assertRaises(WatchError, _watchError)

