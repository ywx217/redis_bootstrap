# coding=utf-8
__author__ = 'ywx217@gmail.com'
import collections

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


class ContainerField(Field):
	# container field represents a set of objects
	def __init__(self, redis_key, var_type, default_factory):
		super(ContainerField, self).__init__(redis_key)
		self.var_type = var_type
		assert callable(default_factory)
		self.default_factory = default_factory

	def _convert(self, val):
		# todo: convert v -> var_type
		return val

	def load(self, mapping, obj):
		# key format: <redis_key>.<index>[.<more_fields>]
		container = self.default_factory()
		prefix = self.redis_key + '.'
		inner_mapping = collections.defaultdict(dict)
		# {index: {field_name: val}}
		for k, v in mapping.iteritems():
			if not k.startswith(prefix):
				continue
			inner_k = k[len(prefix):]
			splits = inner_k.split('.', 1)
			if len(splits) == 2:
				inner_mapping[splits[0]][splits[1]] = v
			else:
				inner_mapping[inner_k][''] = v

		self._add_to(container, inner_mapping)
		obj[self.var_name] = container

	def save(self, save_map, obj):
		# todo: save method to be implemented
		pass

	def _add_to(self, container, mapping):
		raise NotImplementedError


class ListField(ContainerField):
	def __init__(self, redis_key, var_type):
		super(ListField, self).__init__(redis_key, var_type, list)

	def _add_to(self, container, mapping):
		# {index: {field_name: val}}
		for k, v in sorted(mapping.iteritems()):
			i = int(k)
			if i != len(container):
				raise IndexError('index %d cannot add to list container of length %d', i, len(container))
			container.append(self._convert(v))


class MapField(ContainerField):
	def __init__(self, redis_key, var_type):
		super(MapField, self).__init__(redis_key, var_type, dict)

	def _add_to(self, container, mapping):
		# {index: {field_name: val}}
		for k, v in mapping.iteritems():
			container[k] = self._convert(v)
