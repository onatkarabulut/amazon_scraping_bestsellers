import sqlite3
import psycopg2

sqlite_conn = sqlite3.connect('veritabani.db')
sqlite_cursor = sqlite_conn.cursor()

pg_conn = psycopg2.connect(
    dbname='',
    user='',
    password='',
    host='',
    port=''
)
pg_cursor = pg_conn.cursor()

pg_cursor.execute('''
    CREATE TABLE IF NOT EXISTS all_products (
        Link VARCHAR,
        Urun_adi VARCHAR,
        Puan REAL,
        Degerlendirme INTEGER,
        Fiyatlar REAL
    )
''')

sqlite_cursor.execute('SELECT * FROM urunler')
rows = sqlite_cursor.fetchall()

for row in rows:
    # Boş dizeleri kontrol edin ve boşlukları None ile değiştirin
    row = [None if value == '' else value for value in row]
    
    pg_cursor.execute('INSERT INTO amazon_products_scraping (Link, Urun_adi, Puan, Degerlendirme, Fiyatlar) VALUES (%s, %s, %s, %s, %s)', (row[1], row[2], float(row[3]) if row[3] is not None else None, row[4] if row[4] is not None else None, float(row[5]) if row[5] is not None else None))

pg_conn.commit()

pg_conn.close()
sqlite_conn.close()

print("Veri aktarımı tamamlandı.")
