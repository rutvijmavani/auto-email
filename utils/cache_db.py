import sqlite3

conn = sqlite3.connect("job_cache.db")
cursor = conn.cursor()

cursor.execute("SELECT * FROM jobs")
print(cursor.fetchall())