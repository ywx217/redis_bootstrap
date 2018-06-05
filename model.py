# coding=utf-8
from .field import *

__author__ = 'ywx217@gmail.com'


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
		return cls.load_mapping(m, index, read_only)

	@classmethod
	def load_mapping(cls, mapping, index=None, read_only=False):
		obj = cls(index, read_only)
		for field in cls.__fields__:
			field.load(mapping, obj)
		return obj

