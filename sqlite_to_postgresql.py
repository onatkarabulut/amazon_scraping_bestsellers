import sqlite3
import psycopg2

# SQLite veritabanına bağlanın
sqlite_conn = sqlite3.connect('veritabanı.db')
sqlite_cursor = sqlite_conn.cursor()

# PostgreSQL veritabanına bağlanın
pg_conn = psycopg2.connect(
    dbname='',
    user='',
    password='',
    host='',
    port=''
)
pg_cursor = pg_conn.cursor()

sqlite_cursor.execute("PRAGMA table_info(your_table)")
sqlite_columns = [(col[1], col[2]) for col in sqlite_cursor.fetchall()]

pg_cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'tablo_ornek'")
pg_columns = pg_cursor.fetchall()

incompatible_columns = []
for sqlite_column, pg_column in zip(sqlite_columns, pg_columns):
    sqlite_name, sqlite_type = sqlite_column
    pg_name, pg_type = pg_column
    if sqlite_name != pg_name or sqlite_type != pg_type:
        incompatible_columns.append((sqlite_name, sqlite_type, pg_name, pg_type))

if incompatible_columns:
    print("Uyumsuz sütunlar bulundu:")
    for col_info in incompatible_columns:
        print(f"SQLite Sütun: {col_info[0]} ({col_info[1]})")
        print(f"PostgreSQL Sütun: {col_info[2]} ({col_info[3]})")
    print("Veri taşıma işlemi yapılamaz. Uyumsuz sütunları düzeltin.")
else:
    print("Veriler uyumlu. Veri taşıma işlemine başlanabilir.")
sqlite_cursor.execute('SELECT * FROM your_table')
rows = sqlite_cursor.fetchall()

# PostgreSQL'e veri aktarımı
for row in rows:
    pg_cursor.execute('INSERT INTO your_pg_table (column1, column2, column3) VALUES (%s, %s, %s)', row)

pg_conn.commit()
pg_conn.close()
sqlite_conn.close()
