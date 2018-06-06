# coding=utf-8
__author__ = 'ywx217@gmail.com'

import inspect
from .field import *
from .utils import flatten, inflate


class ModelMetaclass(type):
	def __new__(cls, *more):
		name, bases, attrs = more
		if name == 'Model':
			return type.__new__(cls, name, bases, attrs)
		fields = []
		for k, v in attrs.iteritems():
			if isinstance(v, Field):
				v.var_name = k
				fields.append(v)
		for f in fields:
			attrs.pop(f.var_name)
		attrs['__fields__'] = fields
		return type.__new__(cls, name, bases, attrs)


class SubModel(object):
	VAL_FIELD = Field('')

	def __init__(self):
		super(SubModel, self).__init__()

	def save_mapping(self):
		# save current model data into a python dict
		raise NotImplementedError

	@classmethod
	def load_mapping(cls, mapping):
		raise NotImplementedError

	@classmethod
	def make_derived(cls, var_type, **kwargs):
		if inspect.isclass(var_type) and issubclass(var_type, Field):
			var_type = var_type('')
		if not isinstance(var_type, Field):
			raise TypeError('var_type must derived from Field')
		name = '_'.join((cls.__name__, var_type.__class__.__name__))
		kwargs['VAL_FIELD'] = var_type
		newclass = type(name, (cls,), kwargs)
		return newclass


class ListModel(SubModel, list):
	FN_LENGTH = '__len'

	def __init__(self):
		SubModel.__init__(self)
		list.__init__(self)

	def save_mapping(self):
		save_map = {}
		field = self.VAL_FIELD
		length = len(self)
		for i in xrange(length):
			field.var_name = i
			field.redis_key = str(i)
			field.save(save_map, self)
		save_map[self.FN_LENGTH] = length
		return save_map

	@classmethod
	def load_mapping(cls, mapping):
		obj = cls()
		length = int(mapping.get(cls.FN_LENGTH, 0))
		if not length:
			return
		obj.extend((None,) * length)
		field = cls.VAL_FIELD
		for i in xrange(length):
			field.var_name = i
			field.redis_key = str(i)
			field.load(mapping, obj)
		return obj


class DictModel(SubModel, dict):
	KEY_CONV = str

	def __init__(self):
		SubModel.__init__(self)
		dict.__init__(self)

	def save_mapping(self):
		save_map = {}
		field = self.VAL_FIELD
		for k in self:
			field.var_name = k
			field.redis_key = str(k)
			field.save(save_map, self)
		return save_map

	@classmethod
	def load_mapping(cls, mapping):
		obj = cls()
		field = cls.VAL_FIELD
		for k in mapping:
			field.var_name = cls.KEY_CONV(k)
			field.redis_key = k
			field.load(mapping, obj)
		return obj


class Model(dict):
	# Redis saved object, maps redis hash map to object layer
	# Basic use case:
	# > # this enables redis WATCH
	# > o = Model.load(index, read_only=False)
	# > o.foo = 'foo'
	# > o.bar = 'bar'
	# > o.save()

	__metaclass__ = ModelMetaclass
	MODEL_NAME = 'model'

	def __init__(self, index, readonly, **kwargs):
		super(Model, self).__init__(**kwargs)
		self.__readonly = readonly
		self.__name = self.get_name(index)

	def __getattr__(self, item):
		return self[item]

	def __setattr__(self, key, value):
		self[key] = value

	@classmethod
	def get_name(cls, index):
		# return the key for the model saved in redis db
		return ''.join((cls.MODEL_NAME, ':', str(index)))

	def save(self, redis_context):
		# save kv using hmset
		save_map = self.save_mapping()
		save_map = flatten(save_map)
		redis_context.hmset(self.__name, save_map)

	def save_mapping(self):
		if self.__readonly:
			raise ValueError('model %s(%s) is read only!' % (self.__class__.__name__, self.__name))
		save_map = {}
		for field in self.__fields__:
			field.save(save_map, self)
		return save_map

	@classmethod
	def load(cls, redis_context, index, read_only=False):
		# load from redis using hgetall
		name = cls.get_name(index)
		m = redis_context.hgetall(name, watch=not read_only)
		m = inflate(m)
		return cls.load_mapping(m, index, read_only)

	@classmethod
	def load_mapping(cls, mapping, index=None, read_only=False):
		obj = cls(index, read_only)
		for field in cls.__fields__:
			field.load(mapping, obj)
		return obj

