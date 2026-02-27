import sqlite3

conn = sqlite3.connect("quota.db")
cursor = conn.cursor()

cursor.execute("SELECT * FROM model_usage")
print(cursor.fetchall())