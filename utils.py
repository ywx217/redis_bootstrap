# coding=utf-8
__author__ = 'ywx217@gmail.com'

import collections


def flatten(d, parent_key='', sep='.'):
	items = []
	for k, v in d.iteritems():
		new_key = parent_key + sep + k if parent_key else k
		if isinstance(v, collections.MutableMapping):
			items.extend(flatten(v, new_key, sep=sep).items())
		else:
			items.append((new_key, v))
	return dict(items)


def inflate(d, sep='.'):
	items = dict()
	for k, v in d.iteritems():
		keys = k.split(sep)
		sub_items = items
		for ki in keys[:-1]:
			try:
				sub_items = sub_items[ki]
			except KeyError:
				sub_items[ki] = dict()
				sub_items = sub_items[ki]

		sub_items[keys[-1]] = v

	return items
