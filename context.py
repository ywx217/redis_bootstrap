# coding=utf-8
__author__ = 'ywx217@gmail.com'
import redis


class RedisContext(object):
	def __init__(self, redis_client):
		self._redis = redis_client
		self.pipeline = redis_client.pipeline()

	def flush(self):
		self.pipeline.execute()

	def reset(self):
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
		self.pipeline.hset(name, key, value)

	def hmset(self, name, mapping):
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
