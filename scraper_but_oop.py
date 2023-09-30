import pandas as pd
import requests as rq
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
import time

class AmazonScraper:
    def __init__(self, header, max_retries, retry_delay, existing_df):
        self.header = header
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.existing_df = existing_df
        self.urunler_dict = {}

    def scrape_product_info(self, url):
        retry_count = 0
        while retry_count < self.max_retries:
            r = rq.get(url)
            if r.status_code == 200:
                soup = bs(r.content, "lxml")
                ürünler = soup.find_all("div", attrs={"id": "gridItemRoot"})

                for ürün in ürünler:
                    link = self.get_product_link(ürün)
                    self.urunler_dict[link] = self.get_product_data(ürün, link)

                time.sleep(self.retry_delay)
                break
            else:
                retry_count += 1
                print(f"Sayfa yüklenemedi ({retry_count}. deneme). Bekleniyor...")

    def get_product_link(self, ürün):
        link_etiketi = ürün.a
        if link_etiketi:
            url = "https://www.amazon.com.tr" + link_etiketi.get("href")
            return url
        else:
            return "N/A"

    def get_product_data(self, ürün, link):
        urun_adı = self.get_product_name(ürün)
        puan = self.get_product_rating(ürün)
        degerlendirme = self.get_product_reviews(ürün)
        fiyat = self.get_product_price(link)
        return [urun_adı, puan, degerlendirme, fiyat]

    def get_product_name(self, ürün):
        ürün_div = ürün.find("div", attrs={"class": "_cDEzb_p13n-sc-css-line-clamp-3_g3dy1"})
        if ürün_div:
            return ürün_div.text
        else:
            return "N/A"

    def get_product_rating(self, ürün):
        p_ürün = ürün.find('div', class_='a-icon-row')
        if p_ürün:
            puan = p_ürün.find('span', class_='a-icon-alt').text.strip()
            return puan
        else:
            return "N/A"

    def get_product_reviews(self, ürün):
        urun_d = ürün.find("span", attrs={"class": "a-size-small"})
        if urun_d:
            return urun_d.text.strip()
        else:
            return "N/A"

    def get_product_price(self, link):
        while True:
            detay = rq.get(link, headers=self.header)
            detay_soup = bs(detay.content, "lxml")
            fiyat_element = detay_soup.find("span", attrs={"class": "a-offscreen"})

            if fiyat_element:
                return fiyat_element.text
            else:
                time.sleep(2)

    def scrape_amazon_category(self, category_urls):
        for url in tqdm(category_urls, desc="TOPLAM SAYFA"):
            self.scrape_product_info(url)

    def run(self, category_urls):
        self.scrape_amazon_category(category_urls)
        self.save_to_csv()

    def save_to_csv(self):
        df = pd.DataFrame(self.urunler_dict)
        df = df.T
        final_df = pd.concat([self.existing_df, df])
        final_df.to_csv("amazon_products.csv", header=["Urun_adi", "Puan", "Degerlendirme", "Fiyatlar"],
                        index_label="Link", encoding="utf-8")
        print("Ürün bilgileri başarıyla CSV dosyasına kaydedildi.")

if __name__ == "__main__":
    HEADER = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0"}  # Tarayıcınızda geçerli User Agent bilginizi giriniz.
    max_retries = 5  # Kaç kere tekrar deneneceğini ayarlayalım
    retry_delay = 2  # Her tekrar deneme arasındaki süreyi ayarlayalım (saniye cinsinden)

    try:
        existing_df = pd.read_csv("amazon_products.csv")
    except FileNotFoundError:
        existing_df = pd.DataFrame()

    category_urls = [
        "https://www.amazon.com.tr/gp/bestsellers/garden/ref=zg_bs_pg_2_garden?ie=UTF8&pg=",
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

    category_urls = [cat + str(i) for cat in category_urls for i in range(1, 11)] # her kategoriden kaç sayfa yapılacak
    scraper = AmazonScraper(HEADER, max_retries, retry_delay, existing_df)
    scraper.run(category_urls)
