# coding=utf-8
__author__ = 'ywx217@gmail.com'

DEFAULT_PLACEHOLDER = object()


class Field(object):
	__load_method = str

	def __init__(self, redis_key, default=DEFAULT_PLACEHOLDER, default_factory=DEFAULT_PLACEHOLDER):
		self.redis_key = redis_key
		self.var_name = '<missing>'
		self.default = default
		self.default_factory = default_factory

	def save(self, save_map, obj):
		raise NotImplementedError

	def _convert(self, val):
		raise NotImplementedError

	def load(self, mapping, obj):
		val = mapping.get(self.redis_key, None)
		if val is None:
			if self.default is not DEFAULT_PLACEHOLDER:
				val = self.default
			elif self.default_factory is not DEFAULT_PLACEHOLDER and callable(self.default_factory):
				val = self.default_factory()
			else:
				raise KeyError('key not found when loading field %s', self)
		else:
			val = self._convert(val)
		obj[self.var_name] = val

	def __str__(self):
		return '%s<%s, %s>' % (self.__class__.__name__, self.redis_key, self.var_name)

