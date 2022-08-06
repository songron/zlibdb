from email.generator import Generator
import os
import shutil
import tempfile
import unittest
from types import GeneratorType

import zlibdb


class BasicTest(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='zlibdb')
        self.db_path = os.path.join(self.tempdir, 'test.sqlite')
        return super().setUp()

    def tearDown(self):
        shutil.rmtree(self.tempdir)
        return super().tearDown()

    def test_persisted(self):
        db = zlibdb.open(self.db_path)
        db['abc'] = 'hello'
        db.put('xyz', b'world')
        db.close()

        db = zlibdb.open(self.db_path)
        self.assertTrue('abc' in db)
        self.assertEqual(db['abc'], b'hello')
        self.assertTrue('xyz' in db)
        self.assertEqual(db['xyz'], b'world')
        self.assertFalse('nonexist' in db)
        self.assertEqual(db.get('nonexist'), None)
        with self.assertRaises(KeyError):
            db['nonexist']

    def test_deletion(self):
        db = zlibdb.open(self.db_path)
        db['key1'] = 'value1'
        db['key2'] = 'value2'
        self.assertTrue('key1' in db)
        self.assertTrue('key2' in db)
        del db['key1']
        self.assertFalse('key1' in db)
        db.delete('key2')
        self.assertFalse('key2' in db)
        with self.assertRaises(KeyError):
            del db['key1']

    def test_keys(self):
        db = zlibdb.open(self.db_path)
        keys = [str(i) for i in range(10)]
        for k in keys:
            db[k] = 'value'
        _keys = db.keys()
        self.assertTrue(isinstance(_keys, GeneratorType))
        self.assertEqual(list(_keys), keys)

    def test_values(self):
        db = zlibdb.open(self.db_path)
        keys = [str(i) for i in range(10)]
        values = [str(i).encode('utf8') for i in range(10)]
        for k, v in zip(keys, values):
            db[k] = v
        _values = db.values()
        self.assertTrue(isinstance(_values, GeneratorType))
        self.assertEqual(list(_values), values)

    def test_items(self):
        db = zlibdb.open(self.db_path)
        items = [(str(i), str(i).encode('utf8')) for i in range(10)]
        for k, v in items:
            db[k] = v
        _items = db.items()
        self.assertTrue(isinstance(_items, GeneratorType))
        self.assertEqual(list(_items), items)

    def test_iter(self):
        db = zlibdb.open(self.db_path)
        keys = ['a', 'b', 'c']
        for k in keys:
            db[k] = 'value'
        for i, x in enumerate(db):
            self.assertEqual(x, keys[i])

    def test_overwrite(self):
        db = zlibdb.open(self.db_path)
        db['key'] = b'value1'
        db['key'] = b'value2'
        self.assertEqual(db.size(), 1)
        self.assertEqual(db['key'], b'value2')

    def test_with_statement(self):
        with zlibdb.open(self.db_path) as db:
            db['key'] = b'value'
            self.assertEqual(db['key'], b'value')

        with self.assertRaises(AttributeError):
            db['key']

        with db:
            self.assertEqual(db['key'], b'value')
