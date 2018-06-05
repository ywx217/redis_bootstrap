# coding=utf-8
__author__ = 'ywx217@gmail.com'
from redis_bootstrap.tests import BaseTest
from redis_bootstrap.model import Model
from redis_bootstrap.field import *


class PrimitiveModel(Model):
	MODEL_NAME = 'pm'
	val_int = IntField('i')
	val_str = StrField('s')
	val_bool = BoolField('b')
	val_float = FloatField('f')


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
