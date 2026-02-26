import sqlite3
import zlib

# conn = sqlite3.connect("job_cache.db")
# cursor = conn.cursor()

# cursor.execute("SELECT content FROM jobs")
# row = cursor.fetchone()

# print(zlib.decompress(row[0]).decode())

with sqlite3.connect("job_cache.db") as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT content FROM jobs LIMIT 1")
    row = cursor.fetchone()

if not row:
    print("No cached jobs found.")
else:
    print(zlib.decompress(row[0]).decode("utf-8"))