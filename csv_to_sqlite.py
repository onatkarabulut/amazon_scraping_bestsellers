import sqlite3
import csv
import pandas as pd

conn = sqlite3.connect("veritabani.db")
cursor = conn.cursor()

def create_table():
    cursor.execute('''CREATE TABLE IF NOT EXISTS urunler (
                        Link VARCHAR,
                        Urun_adi VARCHAR,
                        Puan FLOAT64,
                        Degerlendirme INTEGER,
                        Fiyatlar FLOAT64
                    )''')

def read_csv(): 
    with open("data/shaped_data.csv", "r") as csv_file: # kendi dosya yolunuzu yazın
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Başlık satırını atla (varsa)
        for row in csv_reader:
            cursor.execute("INSERT INTO urunler (Link, Urun_adi, Puan, Degerlendirme, Fiyatlar) VALUES (?, ?, ?, ?, ?)",
                        (row[0] ,row[1], row[2], row[3], row[4])) 

def run_query(sql):
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        df = pd.DataFrame(result)
        return df
    except Exception as e:
        print("SQL Hatası:", str(e))
        return None

def query():
    print("SQL sorgusu girin (çıkmak için 'exit' yazın):")
    while True:
        sql = input("query> ")
        if sql.lower() == 'exit':
            break
        df = run_query(sql)
        if df is not None:
            print(df)
        else:
            print("Hatalı SQL sorgusu girildi.")


if __name__ == '__main__':
    """
    sql = '''
    SELECT urun_adi FROM urunler WHERE Urun_adi LIKE 'Apple%';
    '''                     
    df = pd.DataFrame(query(sql))
    """
    
    
    create_table()
    read_csv()
    query()    
    conn.commit()
    conn.close()

    
