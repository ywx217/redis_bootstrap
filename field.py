# coding=utf-8
__author__ = 'ywx217@gmail.com'

import re

DEFAULT_PLACEHOLDER = object()
LEGAL_REDIS_KEY = re.compile(r'[a-zA-Z0-9_-]+$')


def check_redis_key(redis_key):
	return bool(LEGAL_REDIS_KEY.match(redis_key))


class Field(object):
	IS_MODEL = False

	def __init__(self, redis_key, default=DEFAULT_PLACEHOLDER, default_factory=DEFAULT_PLACEHOLDER):
		if redis_key == '':
			redis_key = DEFAULT_PLACEHOLDER
		elif not check_redis_key(redis_key):
			raise KeyError('%s is not a valid redis key' % redis_key)
		self.redis_key = redis_key
		self.var_name = '<missing>'
		self.default = default
		if not callable(default_factory):
			default_factory = DEFAULT_PLACEHOLDER
		self.default_factory = default_factory

	def make_default(self):
		if self.default is not DEFAULT_PLACEHOLDER:
			return self.default
		if self.default_factory is not DEFAULT_PLACEHOLDER:
			return self.default_factory()
		return DEFAULT_PLACEHOLDER

	def save(self, save_map, obj):
		raise NotImplementedError

	def convert(self, val):
		raise NotImplementedError

	def load2(self, mapping, obj):
		if self.redis_key is DEFAULT_PLACEHOLDER:
			return
		try:
			val = mapping[self.redis_key]
		except (IndexError, KeyError, TypeError):
			val = self.make_default()
			if val is DEFAULT_PLACEHOLDER:
				raise KeyError('key not found when loading field %s', self)
		else:
			val = self.convert(val)
		obj[self.var_name] = val

	def load(self, mapping, obj):
		if self.redis_key is DEFAULT_PLACEHOLDER:
			return
		val = mapping.get(self.redis_key, None)
		if val is None:
			val = self.make_default()
			if val is DEFAULT_PLACEHOLDER:
				raise KeyError('key not found when loading field %s' % self)
		else:
			val = self.convert(val)
		obj[self.var_name] = val

	def __str__(self):
		return '%s<%s, %s>' % (self.__class__.__name__, self.redis_key, self.var_name)
