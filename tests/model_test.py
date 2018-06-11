# coding=utf-8
__author__ = 'ywx217@gmail.com'

from redis_bootstrap.tests import BaseTest
from redis_bootstrap.model import Model, ObjectSubModel
from redis_bootstrap.field import *
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


class NestedContainerModel(Model):
	MODEL_NAME = 'ncm'
	val_list_list = ListField('ll', ListField('', IntField))
	val_list_map = ListField('lm', DictField('', IntField, key_converter=int))
	val_map_map = DictField('mm', DictField('', StrField))


class PrimitiveSubModel(ObjectSubModel):
	val_int = IntField('i')
	val_str = StrField('s')
	val_bool = BoolField('b')
	val_float = FloatField('f')


class NestedPrimitiveModel(Model):
	MODEL_NAME = 'npm'
	val_list_map = ListField('lm', DictField('', PrimitiveSubModel))


class ModelWithSubModel(Model):
	MODEL_NAME = 'mwsm'
	o1 = SubModelField('o1', PrimitiveSubModel, default=None)
	o2 = SubModelField('o2', PrimitiveSubModel, default=None)


class ModelTest(BaseTest):
	def testFieldName(self):
		self.assertTrue(check_redis_key('foo_bar'))
		self.assertFalse(check_redis_key(''))
		self.assertFalse(check_redis_key('foobar.baz'))
		self.assertFalse(check_redis_key('foobar.'))
		self.assertFalse(check_redis_key('.foobar'))
		self.assertFalse(check_redis_key('$bar'))
		self.assertFalse(check_redis_key('foo$bar'))

	def testModelSaving(self):
		with self.context:
			model = PrimitiveModel(1, False)
			model.val_int = 1
			model.val_str = 'foo'
			model.val_bool = False
			model.val_float = 1234.56
			model.save(self.context)

		loaded_model = PrimitiveModel.load(self.context, 1, True)
		self.assertEqual(model.val_int, loaded_model.val_int)
		self.assertEqual(model.val_str, loaded_model.val_str)
		self.assertEqual(model.val_bool, loaded_model.val_bool)
		self.assertEqual(model.val_float, loaded_model.val_float)

	def testContainerModel(self):
		with self.context:
			model = ContainerModel.load_mapping({}, 1, False)
			model.val_int_list.extend([1, 2, 3])
			model.val_str_list.extend(map(str, [1, 2, 3]))
			model.val_int_map[10] = 10
			model.val_str_map['10'] = '10'
			model.save(self.context)

		loaded_model = ContainerModel.load(self.context, 1, True)
		self.assertEqual(model.val_int_list, loaded_model.val_int_list)
		self.assertEqual(model.val_str_list, loaded_model.val_str_list)
		self.assertEqual(model.val_int_map, loaded_model.val_int_map)
		self.assertEqual(model.val_str_map, loaded_model.val_str_map)

	def testNestedModel(self):
		with self.context:
			model = NestedContainerModel.load_mapping({}, 1, False)
			model.val_list_list.append([1, 2, 3])
			model.val_list_map.append({1: 2})
			model.val_map_map['foo'] = {'bar': 'baz'}
			model.save(self.context)

		loaded_model = NestedContainerModel.load(self.context, 1, True)
		self.assertEqual(model.val_list_list, loaded_model.val_list_list)
		self.assertEqual(model.val_list_map, loaded_model.val_list_map)
		self.assertEqual(model.val_map_map, loaded_model.val_map_map)

	def testNestedCustomType(self):
		with self.context:
			model = NestedPrimitiveModel.load_mapping({}, 1, False)
			model.val_list_map.append({})
			model.val_list_map.append({})
			model.val_list_map[0]['item_in_0'] = {'i': 1, 's': 's', 'b': False, 'f': 100.35}
			model.val_list_map[1]['item_in_1'] = PrimitiveSubModel()
			sub_model = PrimitiveSubModel()
			sub_model.val_int = 2
			sub_model.val_str = 'sss'
			sub_model.val_bool = True
			sub_model.val_float = -0.332
			model.val_list_map[1]['another_item_in_1'] = sub_model
			model.save(self.context)

		loaded_model = NestedPrimitiveModel.load(self.context, 1, True)
		self.assertEqual(model.val_list_map, loaded_model.val_list_map)

	def testModelWithSubModel(self):
		with self.context:
			model = ModelWithSubModel.load_mapping({}, 1, False)
			model.o1 = PrimitiveSubModel()
			model.o1.val_int = 2
			model.save(self.context)

		loaded_model = ModelWithSubModel.load(self.context, 1, True)
		self.assertEqual(model.o1, loaded_model.o1)
		self.assertIsNotNone(model.o1)
		self.assertIsNotNone(loaded_model.o1)
		self.assertIsNone(model.o2)
		self.assertIsNone(loaded_model.o2)
