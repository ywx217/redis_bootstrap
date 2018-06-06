# coding=utf-8
__author__ = 'ywx217@gmail.com'

from .field import Field, DEFAULT_PLACEHOLDER
from .model import SubModel, DictModel, ListModel


class PrimitiveField(Field):
	def save(self, save_map, obj):
		if self.redis_key is DEFAULT_PLACEHOLDER:
			return
		save_map[self.redis_key] = obj[self.var_name]

	def convert(self, val):
		raise NotImplementedError


class BoolField(PrimitiveField):
	def __init__(self, redis_key, default=False, default_factory=DEFAULT_PLACEHOLDER):
		super(BoolField, self).__init__(redis_key, default, default_factory)

	def convert(self, val):
		return bool(int(val))

	def save(self, save_map, obj):
		if self.redis_key is DEFAULT_PLACEHOLDER:
			return
		save_map[self.redis_key] = int(obj[self.var_name])


class IntField(PrimitiveField):
	def __init__(self, redis_key, default=0, default_factory=DEFAULT_PLACEHOLDER):
		super(IntField, self).__init__(redis_key, default, default_factory)

	def convert(self, val):
		return int(val)


class FloatField(PrimitiveField):
	def __init__(self, redis_key, default=0.0, default_factory=DEFAULT_PLACEHOLDER):
		super(FloatField, self).__init__(redis_key, default, default_factory)

	def convert(self, val):
		return float(val)


class StrField(PrimitiveField):
	def __init__(self, redis_key, default='', default_factory=DEFAULT_PLACEHOLDER):
		super(StrField, self).__init__(redis_key, default, default_factory)

	def convert(self, val):
		return str(val)


class SubModelField(Field):
	IS_MODEL = True

	def __init__(self, redis_key, model_class, default=DEFAULT_PLACEHOLDER, default_factory=DEFAULT_PLACEHOLDER):
		super(SubModelField, self).__init__(redis_key, default, default_factory)
		self.model_class = model_class
		if not issubclass(model_class, SubModel):
			raise TypeError('Model class %s must inherit from SubModel' % model_class)

	def save(self, save_map, obj):
		if self.redis_key is DEFAULT_PLACEHOLDER:
			return
		save_map[self.redis_key] = obj[self.var_name].save_mapping()

	def convert(self, val):
		if val is None:
			return None
		return self.model_class.load_mapping(val)


class DictField(SubModelField):
	def __init__(self, redis_key, var_type, key_converter=str):
		model_class = DictModel.make_model_class(var_type, KEY_CONV=key_converter)
		super(DictField, self).__init__(redis_key, model_class, DEFAULT_PLACEHOLDER, model_class)


class ListField(SubModelField):
	def __init__(self, redis_key, var_type):
		model_class = ListModel.make_model_class(var_type)
		super(ListField, self).__init__(redis_key, model_class, DEFAULT_PLACEHOLDER, model_class)
