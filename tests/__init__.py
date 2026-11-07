# Importing the tests package pins DATABASE_URL to a verified test database
# (name must contain "test") before any module imports db.connection.
# See tests/_db_guard.py.
from ._db_guard import enforce_test_database as _enforce_test_database

_enforce_test_database()
