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
        id SERIAL PRIMARY KEY,
        Link text,
        Urun_adi text,
        Puan float,
        Degerlendirme int,
        Fiyatlar float
    )
''')

sqlite_cursor.execute('SELECT * FROM urunler')
rows = sqlite_cursor.fetchall()

for row in rows:
    # Boş dizeleri kontrol edin ve boşlukları None ile değiştirin
    row = [None if value == '' else value for value in row]
    
    pg_cursor.execute('INSERT INTO all_products (id, Link, Urun_adi, Puan, Degerlendirme, Fiyatlar) VALUES (%s, %s, %s, %s, %s, %s)', (row[0] ,row[1], row[2], row[3], row[4], row[5]))

pg_conn.commit()

pg_conn.close()
sqlite_conn.close()

print("Veri aktarımı tamamlandı.")
