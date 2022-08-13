import sqlite3
import zlib


class ZlibDB:
    def __init__(self, db_path, encoding='utf-8', level=9):
        self.db_path = db_path
        self.encoding = encoding
        self.level = level
        self._connect()

    def _connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS kv (key TEXT UNIQUE, value BLOB)')

    def close(self):
        self.conn.commit()
        self.conn.close()
        self.conn = None

    def commit(self):
        self.conn.commit()

    def get(self, key):
        """Returns the (decompressed) value for the specific key."""
        row = self.conn.execute(
            'SELECT value FROM kv WHERE key = ?', (key,)).fetchone()
        return None if row is None else zlib.decompress(row[0])

    def put(self, key, value, level=None):
        """Saves the value for the specific key. 

        The `value` is compressed using zlib by the specific 
        compression `level` before written to the db.
        """
        if level is None:
            level = self.level
        if isinstance(value, str):
            value = value.encode(self.encoding)
        if not isinstance(value, bytes):
            raise TypeError('value must be bytes or string')
        value = zlib.compress(value, level=level)
        self.conn.execute(
            'REPLACE INTO kv (key, value) VALUES (?,?)', (key, value))

    def delete(self, key):
        """Deletes the items for the specific key."""
        self.conn.execute('DELETE FROM kv WHERE key = ?', (key,))

    def size(self):
        """Returns the number of items in the db."""
        return self.conn.execute('SELECT COUNT(1) FROM kv').fetchone()[0]

    def keys(self):
        """Generator of keys."""
        c = self.conn.cursor()
        for row in c.execute('SELECT key FROM kv'):
            yield row[0]

    def values(self):
        """Generator of values."""
        c = self.conn.cursor()
        for row in c.execute('SELECT value FROM kv'):
            yield zlib.decompress(row[0])

    def items(self):
        """Generator of (key, value) tuples."""
        c = self.conn.cursor()
        for row in c.execute('SELECT key, value FROM kv'):
            yield (row[0], zlib.decompress(row[1]))

    def range(self, start, end):
        """Generator of (key, value) tuples in the key range [start, end)."""
        c = self.conn.cursor()
        for row in c.execute(
            'SELECT key, value FROM kv WHERE key >= ? AND key < ? ORDER BY key ASC',
            (start, end)
        ):
            yield (row[0], zlib.decompress(row[1]))

    def __contains__(self, key):
        """Returns True if the key exists in the db; False otherwise."""
        row = self.conn.execute(
            'SELECT 1 FROM kv WHERE key = ?', (key,)).fetchone()
        return row is not None

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __setitem__(self, key, value):
        self.put(key, value)

    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        self.delete(key)

    def __iter__(self):
        return self.keys()

    def __len__(self):
        return self.size()

    def __enter__(self):
        if self.conn is None:
            self._connect()
        return self

    def __exit__(self, *exc_info):
        self.close()


def open(*args, **kwargs):
    """Returns an instance of ZlibDB."""
    return ZlibDB(*args, **kwargs)
