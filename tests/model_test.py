# coding=utf-8
__author__ = 'ywx217@gmail.com'

from redis_bootstrap.tests import BaseTest
from redis_bootstrap.model import Model
from redis_bootstrap.field_implements import *


class PrimitiveModel(Model):
	MODEL_NAME = 'pm'
	val_int = IntField('i')
	val_str = StrField('s')
	val_bool = BoolField('b')
	val_float = FloatField('f')


class ContainerModel(Model):
	MODEL_NAME = 'cm'
	val_int_list = ListField('il', IntField(''))
	val_str_list = ListField('sl', StrField)
	val_int_map = DictField('im', IntField, key_converter=int)
	val_str_map = DictField('sm', StrField)


class ModelTest(BaseTest):
	def testModelSaving(self):
		model = PrimitiveModel(1, False)
		model.val_int = 1
		model.val_str = 'foo'
		model.val_bool = False
		model.val_float = 1234.56
		model.save(self.context)
		self.context.flush()

		loaded_model = PrimitiveModel.load(self.context, 1, True)
		self.assertEqual(model.val_int, loaded_model.val_int)
		self.assertEqual(model.val_str, loaded_model.val_str)
		self.assertEqual(model.val_bool, loaded_model.val_bool)
		self.assertEqual(model.val_float, loaded_model.val_float)

	def testContainerModelSaving(self):
		model = ContainerModel.load_mapping({}, 1, False)
		model.val_int_list.extend([1, 2, 3])
		model.val_str_list.extend(map(str, [1, 2, 3]))
		model.val_int_map[10] = 10
		model.val_str_map['10'] = '10'
		model.save(self.context)
		self.context.flush()

		loaded_model = ContainerModel.load(self.context, 1, True)
		self.assertEqual(model.val_int_list, loaded_model.val_int_list)
		self.assertEqual(model.val_str_list, loaded_model.val_str_list)
		self.assertEqual(model.val_int_map, loaded_model.val_int_map)
		self.assertEqual(model.val_str_map, loaded_model.val_str_map)
