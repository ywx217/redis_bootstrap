# coding=utf-8
__author__ = 'ywx217@gmail.com'

import redis
from redis import RedisError


class RedisContext(object):
	def __init__(self, redis_client):
		self._redis = redis_client
		self.pipeline = redis_client.pipeline()
		self._in_transaction = False

	def __enter__(self):
		self.begin()

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.flush()

	def is_in_transaction(self):
		return self._in_transaction

	def begin(self):
		if self._in_transaction:
			raise RedisError('already in transaction')
		self._in_transaction = True
		self.pipeline.multi()

	def flush(self):
		if not self._in_transaction:
			raise RedisError('not in transaction')
		self._in_transaction = False
		if not self.pipeline.command_stack:
			self.pipeline.reset()
		else:
			self.pipeline.execute()

	def reset(self):
		self._in_transaction = False
		self.pipeline.reset()

	def watch(self, *keys):
		self.pipeline.watch(*keys)

	# --------------- hash map ---------------
	def hget(self, name, key, watch=True):
		if watch:
			self.pipeline.watch(name)
		return self._redis.hget(name, key)

	def hmget(self, name, keys, watch=True):
		if watch:
			self.pipeline.watch(name)
		return self._redis.hmget(name, keys)

	def hgetall(self, name, watch=True):
		if watch:
			self.pipeline.watch(name)
		return self._redis.hgetall(name)

	def hset(self, name, key, value):
		if not self._in_transaction:
			self._redis.hset(name, key, value)
		else:
			self.pipeline.hset(name, key, value)

	def hmset(self, name, mapping):
		if not self._in_transaction:
			self._redis.hmset(name, mapping)
		else:
			self.pipeline.hmset(name, mapping)


class RedisSingleton(type):
	_instances = {}

	def __call__(cls, *args, **kwargs):
		db_name = args[0]
		if (cls, db_name) not in cls._instances:
			cls._instances[(cls, args)] = super(RedisSingleton, cls).__call__(*args, **kwargs)
		return cls._instances[(cls, args)]


class RedisManager(object):
	__metaclass__ = RedisSingleton

	def __init__(self, host, port, db, max_connections=20):
		super(RedisManager, self).__init__()
		pool = redis.BlockingConnectionPool(host=host, port=port, db=db, max_connections=max_connections)
		self._redis = redis.StrictRedis(connection_pool=pool)

	def create_context(self):
		return RedisContext(self._redis)

	def get_redis(self):
		return self._redis
