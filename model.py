# coding=utf-8
__author__ = 'ywx217@gmail.com'

import inspect
import collections
import itertools
from .field import *
from .utils import flatten, inflate


class ModelMetaclass(type):
	def __new__(mcs, *more):
		name, bases, attrs = more
		if name == 'Model':
			return type.__new__(mcs, name, bases, attrs)
		fields = []
		for k, v in attrs.iteritems():
			if isinstance(v, Field):
				v.var_name = k
				fields.append(v)
		for f in fields:
			attrs.pop(f.var_name)
		attrs['__fields__'] = fields
		return type.__new__(mcs, name, bases, attrs)


class SubModel(object):
	def save_mapping(self):
		# save current model data into a python dict
		raise NotImplementedError

	@classmethod
	def load_mapping(cls, mapping):
		raise NotImplementedError


class ObjectSubModel(SubModel, dict):
	__metaclass__ = ModelMetaclass

	def __init__(self):
		SubModel.__init__(self)
		dict.__init__(self)
		# init defaults
		for field in self.__fields__:
			if not field.var_name:
				continue
			val = field.make_default()
			if val is DEFAULT_PLACEHOLDER:
				continue
			self[field.var_name] = val

	def __getattr__(self, item):
		return self[item]

	def __setattr__(self, key, value):
		self[key] = value

	def save_mapping(self):
		save_map = {}
		for field in self.__fields__:
			field.save(save_map, self)
		return save_map

	@classmethod
	def load_mapping(cls, mapping):
		obj = cls()
		for field in cls.__fields__:
			field.load(mapping, obj)
		return obj


class ContainerSubModel(SubModel):
	VAL_FIELD = Field('')

	def __init__(self):
		super(ContainerSubModel, self).__init__()
		self.check_model = self.VAL_FIELD.IS_MODEL

	def _convert_item(self, val):
		if self.check_model and not isinstance(val, SubModel):
			return self.VAL_FIELD.convert(val)
		return val

	def save_mapping(self):
		# save current model data into a python dict
		raise NotImplementedError

	@classmethod
	def load_mapping(cls, mapping):
		raise NotImplementedError

	@classmethod
	def make_model_class(cls, var_type, **kwargs):
		if inspect.isclass(var_type):
			if issubclass(var_type, SubModel):
				from .field_implements import SubModelField
				var_type = SubModelField('', var_type)
			elif issubclass(var_type, Field):
				var_type = var_type('')
		if not isinstance(var_type, Field):
			raise TypeError('var_type must derived from Field')
		name = '_'.join((cls.__name__, var_type.__class__.__name__))
		kwargs['VAL_FIELD'] = var_type
		newclass = type(name, (cls,), kwargs)
		return newclass


class ListModel(ContainerSubModel, list):
	FN_LENGTH = '__len'

	def __init__(self):
		ContainerSubModel.__init__(self)
		list.__init__(self)

	def __setitem__(self, i, o):
		list.__setitem__(self, i, self._convert_item(o))

	def append(self, obj):
		list.append(self, self._convert_item(obj))

	def extend(self, iterable):
		list.extend(self, map(self._convert_item, iterable))

	def insert(self, index, obj):
		list.insert(self, index, self._convert_item(obj))

	def __setslice__(self, start, stop, o):
		list.__setslice__(self, start, stop, map(self._convert_item, o))

	def __add__(self, x):
		return list.__add__(self, map(self._convert_item, x))

	def __iadd__(self, x):
		return list.__iadd__(self, map(self._convert_item, x))

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
	def load_sequence(cls, sequence):
		obj = cls()
		length = len(sequence)
		obj.extend((None,) * length)
		field = cls.VAL_FIELD
		for i in xrange(length):
			field.var_name = i
			field.redis_key = i
			field.load2(sequence, obj)
		for i, v in enumerate(obj):
			if v is None:
				raise ValueError('%s[%d] is None' % (cls.__name__, i))
		return obj

	@classmethod
	def load_mapping(cls, mapping):
		if isinstance(mapping, collections.Sequence):
			return cls.load_sequence(mapping)
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
		for i, v in enumerate(obj):
			if v is None:
				raise ValueError('%s[%d] is None' % (cls.__name__, i))
		return obj


class DictModel(ContainerSubModel, dict):
	KEY_CONV = str

	def __init__(self):
		ContainerSubModel.__init__(self)
		dict.__init__(self)

	def __setitem__(self, k, v):
		return dict.__setitem__(self, k, self._convert_item(v))

	def setdefault(self, k, default=None):
		v = self.get(k, DEFAULT_PLACEHOLDER)
		if v is DEFAULT_PLACEHOLDER:
			v = self._convert_item(default)
			self[k] = v
		return v

	def update(self, m, **kwargs):
		for k, v in itertools.chain(m.iteritems(), kwargs.iteritems()):
			self[k] = self._convert_item(v)

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
		# init defaults
		for field in self.__fields__:
			if not field.var_name:
				continue
			val = field.make_default()
			if val is DEFAULT_PLACEHOLDER:
				continue
			self[field.var_name] = val

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
		if not redis_context.is_in_transaction():
			raise ValueError('Saving model out of transaction.')
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
	def load(cls, redis_context, index, read_only=False, can_be_none=False):
		# load from redis using hgetall
		name = cls.get_name(index)
		m = redis_context.hgetall(name, watch=not read_only)
		if can_be_none and not m:
			return None
		m = inflate(m)
		return cls.load_mapping(m, index, read_only)

	@classmethod
	def load_mapping(cls, mapping, index=None, read_only=False):
		obj = cls(index, read_only)
		for field in cls.__fields__:
			field.load(mapping, obj)
		return obj

	@classmethod
	def exists(cls, redis_context, index):
		return redis_context.exists(cls.get_name(index))

	@classmethod
	def delete(cls, redis_context, index):
		return redis_context.delete(cls.get_name(index))
