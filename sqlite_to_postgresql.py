import sqlite3
import psycopg2
from psycopg2 import sql

# Bağlantı bilgilerini bir yapılandırma dosyasından okuyun
sqlite_config = {
    'dbname': 'veritabanı.db',
}

pg_config = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': '',
}

def create_connection(config):
    try:
        conn = psycopg2.connect(**config)
        return conn
    except psycopg2.Error as e:
        print(f"Hata: PostgreSQL bağlantısı oluşturulamadı: {e}")
        return None

def compare_columns(sqlite_cursor, pg_cursor):
    # Sütunları karşılaştırın ve uyumsuz sütunları listeleyin
    incompatible_columns = []
    sqlite_columns = [(col[1], col[2]) for col in sqlite_cursor.execute("PRAGMA table_info(your_table)").fetchall()]
    pg_columns = pg_cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'your_pg_table'").fetchall()
    
    for sqlite_column, pg_column in zip(sqlite_columns, pg_columns):
        sqlite_name, sqlite_type = sqlite_column
        pg_name, pg_type = pg_column
        if sqlite_name != pg_name or sqlite_type != pg_type:
            incompatible_columns.append((sqlite_name, sqlite_type, pg_name, pg_type))
    
    return incompatible_columns

def main():
    sqlite_conn = sqlite3.connect(sqlite_config['dbname'])
    sqlite_cursor = sqlite_conn.cursor()
    
    pg_conn = create_connection(pg_config)
    if pg_conn is None:
        return
    
    pg_cursor = pg_conn.cursor()

    incompatible_columns = compare_columns(sqlite_cursor, pg_cursor)
    
    if incompatible_columns:
        print("Uyumsuz sütunlar bulundu:")
        for col_info in incompatible_columns:
            print(f"SQLite Sütun: {col_info[0]} ({col_info[1]})")
            print(f"PostgreSQL Sütun: {col_info[2]} ({col_info[3]})")
        print("Veri taşıma işlemi yapılamaz. Uyumsuz sütunları düzeltin.")
    else:
        print("Veriler uyumlu. Veri taşıma işlemine başlanabilir.")
        
        # SQLite'den veri alın
        sqlite_cursor.execute('SELECT * FROM your_table')
        rows = sqlite_cursor.fetchall()
        
        # PostgreSQL'e veri aktarımı
        for row in rows:
            insert_query = sql.SQL('INSERT INTO your_pg_table (column1, column2, column3) VALUES ({}, {}, {})').format(
                sql.Literal(row[0]), sql.Literal(row[1]), sql.Literal(row[2])
            )
            pg_cursor.execute(insert_query)
        
        pg_conn.commit()
        print("Veri aktarımı başarıyla tamamlandı.")
    
    sqlite_conn.close()
    pg_conn.close()

if __name__ == "__main__":
    main()
