import sqlite3
import zlib

conn = sqlite3.connect("job_cache.db")
cursor = conn.cursor()

cursor.execute("SELECT content FROM jobs")
row = cursor.fetchone()

print(zlib.decompress(row[0]).decode())