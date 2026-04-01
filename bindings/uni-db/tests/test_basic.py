import os
import shutil
import sys
import tempfile
import time
import unittest

# Ensure we can import the module from the current directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uni_db


class TestUni(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="test_db_python_")
        self.db = uni_db.Uni.open(self.test_dir)
        self.session = self.db.session()

    def tearDown(self):
        # We can't easily close the DB in the current bindings,
        # so we might fail to clean up if the DB holds locks.
        # But Uni is embedded, so it should drop when the object is collected.
        del self.session
        del self.db
        self._rmtree_with_retries(self.test_dir)

    def _rmtree_with_retries(self, path, attempts=8, delay=0.05):
        for attempt in range(attempts):
            try:
                shutil.rmtree(path)
                return
            except FileNotFoundError:
                return
            except OSError:
                if attempt == attempts - 1:
                    raise
                time.sleep(delay)

    def test_basic_query(self):
        # Create schema
        self.db.schema().label("Person").property("name", "string").property(
            "age", "int"
        ).apply()

        # Create a node
        self.session.query("CREATE (n:Person {name: 'Alice', age: 30})")

        # Query it back
        results = self.session.query(
            "MATCH (n:Person) RETURN n.name as name, n.age as age"
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "Alice")
        self.assertEqual(results[0]["age"], 30)

    def test_params(self):
        # Create schema
        self.db.schema().label("Person").property("name", "string").property(
            "age", "int"
        ).apply()

        # Create using params
        params = {"name": "Bob", "age": 25}
        self.session.query("CREATE (n:Person {name: $name, age: $age})", params)

        # Query back
        # Note: returning 'n' might return VID string in current vectorized engine.
        # We return specific properties to verify params worked.
        results = self.session.query(
            "MATCH (n:Person {name: 'Bob'}) RETURN n.name as name, n.age as age"
        )
        self.assertEqual(len(results), 1)
        row = results[0]
        self.assertEqual(row["name"], "Bob")
        self.assertEqual(row["age"], 25)

    def test_list_and_map(self):
        self.db.schema().label("Item").property("tags", "list:string").apply()

        # Test passing a list parameter.
        self.session.query("CREATE (n:Item {tags: $tags})", {"tags": ["a", "b"]})

        results = self.session.query("MATCH (n:Item) RETURN n.tags as tags")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["tags"], ["a", "b"])


if __name__ == "__main__":
    unittest.main()
