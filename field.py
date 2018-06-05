# coding=utf-8
__author__ = 'ywx217@gmail.com'

DEFAULT_PLACEHOLDER = object()


class Field(object):
	__load_method = str

	def __init__(self, redis_key, default=DEFAULT_PLACEHOLDER):
		self.redis_key = redis_key
		self.var_name = '<missing>'
		self.default = default

	def save(self, save_map, obj):
		raise NotImplementedError

	def _convert(self, val):
		raise NotImplementedError

	def load(self, mapping, obj):
		if self.default is DEFAULT_PLACEHOLDER:
			val = mapping[self.redis_key]
		else:
			val = mapping.get(self.redis_key, self.default)
		obj[self.var_name] = self._convert(val)


class PrimitiveField(Field):
	def save(self, save_map, obj):
		save_map[self.redis_key] = obj[self.var_name]

	def _convert(self, val):
		raise NotImplementedError


class BoolField(PrimitiveField):
	def _convert(self, val):
		return bool(int(val))

	def save(self, save_map, obj):
		save_map[self.redis_key] = int(obj[self.var_name])


class IntField(PrimitiveField):
	def _convert(self, val):
		return int(val)


class FloatField(PrimitiveField):
	def _convert(self, val):
		return float(val)


class StrField(PrimitiveField):
	def _convert(self, val):
		return str(val)
