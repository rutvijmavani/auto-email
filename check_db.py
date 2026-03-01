import sqlite3
import zlib

conn = sqlite3.connect('data/recruiter_pipeline.db')
c = conn.cursor()
c.execute('SELECT job_url, created_at, content FROM jobs')

for row in c.fetchall():
    print('URL:', row[0])
    print('Created:', row[1])
    print('Content:')
    print(zlib.decompress(row[2]).decode('utf-8'))
    print('=' * 55)

conn.close()