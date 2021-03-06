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

	def unsafe_get_redis(self):
		return self._redis

	def is_in_transaction(self):
		return self._in_transaction

	def flush_and_begin(self):
		self.flush()
		self.begin()

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
			self.pipeline.reset()

	def reset(self):
		self._in_transaction = False
		self.pipeline.reset()

	def watch(self, *keys):
		self.pipeline.watch(*keys)

	# --------------- other ops ---------------
	def scan_iter(self, match=None, count=None):
		return self._redis.scan_iter(match=match, count=count)

	def incr(self, key, amount=1):
		return self._redis.incr(key, amount)

	def set(self, key, value, ex=None, px=None, nx=False, xx=False):
		if not self._in_transaction:
			return self._redis.set(key, value, ex, px, nx, xx)
		return self.pipeline.set(key, value, ex, px, nx, xx)

	def get(self, key, watch=False):
		if watch:
			self.pipeline.watch(key)
		return self._redis.get(key)

	def lpush(self, key, *values):
		return self._redis.lpush(key, *values)

	def rpop(self, key):
		return self._redis.rpop(key)

	def lrem(self, name, count, value):
		return self._redis.lrem(name, count, value)

	def brpop(self, key, timeout=0):
		return self._redis.brpop(key, timeout=timeout)

	def rpush(self, key, *values):
		return self._redis.rpush(key, *values)

	def zadd(self, key, *score_and_members):
		return self._redis.zadd(key, *score_and_members)

	def zrem(self, key, *values):
		return self._redis.zrem(key, *values)

	def zrange(self, key, start, end, desc=False, withscores=False, score_cast_func=float):
		return self._redis.zrange(key, start, end, desc=desc, withscores=withscores, score_cast_func=score_cast_func)

	def exists(self, name):
		return self._redis.exists(name)

	def delete(self, *names):
		if not self._in_transaction:
			self._redis.delete(*names)
		else:
			self.pipeline.delete(*names)

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
		if (cls, args) not in cls._instances:
			cls._instances[(cls, args)] = super(RedisSingleton, cls).__call__(*args, **kwargs)
		return cls._instances[(cls, args)]


class RedisManager(object):
	__metaclass__ = RedisSingleton

	def __init__(self, host, port, db, max_connections=100):
		super(RedisManager, self).__init__()
		pool = redis.BlockingConnectionPool(host=host, port=port, db=db, max_connections=max_connections)
		self._redis = redis.StrictRedis(connection_pool=pool)

	def create_context(self):
		return RedisContext(self._redis)

	def get_redis(self):
		return self._redis
