import unittest
from database import handle_mongodb_errors
import pymongo

class TestAutoReconnectHandler(unittest.TestCase):

    def test_success(self):
        self.attempt = 0

        @handle_mongodb_errors
        def mongodb_call(self):
            if self.attempt >= 3:
                return True
            self.attempt += 1
            raise pymongo.errors.AutoReconnect
        
        self.assertTrue(mongodb_call(self))

    def test_failure(self):
        @handle_mongodb_errors
        def mongodb_call():
            raise pymongo.errors.AutoReconnect

        with self.assertRaises(pymongo.errors.AutoReconnect):
            mongodb_call()


if __name__ == '__main__':
    unittest.main()

