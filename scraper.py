import pandas as pd
import requests as rq
from bs4 import BeautifulSoup as bs
from tqdm import tqdm 
import time

# Bu veri kazıma işlemi 28 Eylül 2023 tarihinde yapılmıştır. Amazon web kodlarının değişmesi durumunda manuel olarak kontrol edilmelidir.
# Önce mevcut veriyi içe aktarın veya yeni bir DataFrame oluşturalım
HEADER = {"User-Agent": " ... "} # Tarayıcınızda geçerli User Agent bilginizi giriniz.

try:
    existing_df = pd.read_csv("amazon_products.csv")
except FileNotFoundError:
    existing_df = pd.DataFrame()
    
categorys = ["https://www.amazon.com.tr/gp/bestsellers/garden/ref=zg_bs_pg_2_garden?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/baby-products/ref=zg_bs_pg_2_baby-products?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/computers/ref=zg_bs_pg_2_computers?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/electronics/ref=zg_bs_pg_2_electronics?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/home/ref=zg_bs_pg_2_home?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/pet-supplies/ref=zg_bs_pg_2_pet-supplies?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/grocery/ref=zg_bs_pg_2_grocery?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/gift-cards/ref=zg_bs_pg_2_gift-cards?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/beauty/ref=zg_bs_pg_2_beauty?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/books/ref=zg_bs_pg_2_books?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/apparel/ref=zg_bs_pg_2_apparel?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/kitchen/ref=zg_bs_pg_2_kitchen?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/musical-instruments/ref=zg_bs_pg_2_musical-instruments?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/office-products/ref=zg_bs_pg_2_office-products?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/automotive/ref=zg_bs_pg_2_automotive?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/toys/ref=zg_bs_pg_2_toys?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/sporting-goods/ref=zg_bs_pg_2_sporting-goods?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/videogames/ref=zg_bs_pg_2_videogames?ie=UTF8&pg=",
             "https://www.amazon.com.tr/gp/bestsellers/home-improvement/ref=zg_bs_pg_2_home-improvement?ie=UTF8&pg="
            ]
categorys = [cat + str(i) for cat in categorys for i in range(1, 11)] # her kategoriden kaç sayfa yapılacak

max_retries = 5  # Kaç kere tekrar deneneceğini ayarlayalım
retry_delay = 2  # Her tekrar deneme arasındaki süreyi ayarlayalım (saniye cinsinden)
urunler_dict = {}

for url in tqdm(categorys, desc="TOPLAM SAYFA"):
    retry_count = 0
    while retry_count < max_retries:
        r = rq.get(url)
        if r.status_code == 200:
            r = rq.get(url)
            soup = bs(r.content, "lxml")
            r.status_code

            ürünler = soup.find_all("div", attrs={"id":"gridItemRoot"})

            def isimler(ürün):
                ürün_div = ürün.find("div", attrs={"class":"_cDEzb_p13n-sc-css-line-clamp-3_g3dy1"})   
                if ürün_div:
                    ürün_adı = ürün_div.text
                    return ürün_adı
                else:
                    return "N/A"
                

            def puanlar(ürün):
                p_ürün = ürün.find('div', class_='a-icon-row')
                if p_ürün:
                    puan = p_ürün.find('span', class_='a-icon-alt').text.strip()
                    return puan
                else:
                    return "N/A"
                
            def degerlendirmeler(ürün):
                urun_d = ürün.find("span", attrs={"class":"a-size-small"})
                if urun_d:
                    return urun_d.text.strip()
                else:
                    return "N/A"

            def links(ürün):
                link_etiketi = ürün.a
                if link_etiketi:
                    url = "https://www.amazon.com.tr" + link_etiketi.get("href")
                    return url
                else:
                    return "N/A"
                
            def fiyats(link):
                while True:  # Sonsuz bir döngü oluşturduk
                    detay = rq.get(link, headers=HEADER)
                    detay_soup = bs(detay.content, "lxml")
                    fiyat_element = detay_soup.find("span", attrs={"class": "a-offscreen"})
                    
                    if fiyat_element:
                        fiyat = fiyat_element.text
                        return fiyat  # Fiyat bulundu, döngüyü sonlandır
                    else:
                        time.sleep(2)  # Fiyat bulunamadı, 2 saniye bekle ve tekrar dene

            for ürün in ürünler:
                link = links(ürün)
                urunler_dict[link] = [isimler(ürün), 
                                      puanlar(ürün), 
                                      degerlendirmeler(ürün),
                                      fiyats(link)]
                #print(urunler_dict) # sözlüklerin yazılmasını kontrol etmek için yorum satırından alabilirsiniz.

            time.sleep(retry_delay)
            break
        else:
            retry_count += 1
            print(f"Sayfa yüklenemedi ({retry_count}. deneme). Bekleniyor...")
            
# Tüm ürün bilgilerini bir DataFrame'e dönüştürdük
df = pd.DataFrame(urunler_dict)
df = df.T  # Transpozunu alarak satırları sütunlara dönüştürdül

# Mevcut veri ile birleştirdik
final_df = pd.concat([existing_df, df])

final_df.to_csv("amazon_products.csv", header=["Urun_adi", "Puan", "Degerlendirme", "Fiyatlar"], index_label="Link", encoding="utf-8")

print("Ürün bilgileri başarıyla CSV dosyasına kaydedildi.")
