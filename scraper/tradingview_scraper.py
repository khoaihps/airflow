from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import logging
from bs4 import BeautifulSoup
import requests
import time

from common.db import PostgresDB

BASE_URL = "https://www.tradingview.com"
TARGET_URL = f"{BASE_URL}/symbols/BTCUSD/news/?exchange=CRYPTO"
HEADERS = {"User-Agent": "Mozilla/5.0"}

class TradingViewScraper:
    def __init__(self):
        self.db = PostgresDB()

    def init_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")

        return webdriver.Remote(
            command_executor="http://selenium:4444/wd/hub",
            options=options,
        )

    def get_cached_urls(self):
        rows = self.db.execute("SELECT url FROM cache_tradingview")
        return set(row["url"] for row in rows) if rows else set()

    def save_urls_to_cache(self, urls):
        for url in urls:
            self.db.execute(f"""
                INSERT INTO cache_tradingview (url) VALUES ('{url}')
                ON CONFLICT DO NOTHING
            """)

    def scrape_article_links(self):
        driver = self.init_driver()
        driver.get(TARGET_URL)

        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[href^='/news/']"))
            )
            a_tags = driver.find_elements(By.CSS_SELECTOR, "a[href^='/news/']")
            links = list({a.get_attribute("href") for a in a_tags if a.get_attribute("href")})
        except Exception as e:
            logging.error("No article links found")
            links = []
        finally:
            driver.quit()

        logging.info(f"Found {len(links)} links")
        return links

    def parse_article(self, url):
        try:
            logging.info(f"Parsing {url}")
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            title_tag = soup.find("h1")
            title = title_tag.get_text(strip=True) if title_tag else "No title"

            time_tag = soup.find("time-format")
            timestamp = int(time_tag["timestamp"]) / 1000 if time_tag and time_tag.has_attr("timestamp") else None

            content_div = soup.find("div", class_="content-KX2tCBZq")
            content = "\n".join(p.get_text(strip=True) for p in content_div.find_all("p")) if content_div else "No content"

            return {
                "url": url,
                "title": title,
                "timestamp": timestamp,
                "content": content
            }
        except Exception as e:
            logging.error(f"Error scraping {url}: {e}")
            return None

    def run(self, max_articles=5):
        all_links = self.scrape_article_links()
        cached = self.get_cached_urls()
        new_links = [url for url in all_links if url not in cached]

        logging.info(f"🆕 {len(new_links)} new articles to scrape")

        results = []
        scraped_urls = []

        for url in new_links[:max_articles]:
            article = self.parse_article(url)
            if article:
                results.append(article)
                scraped_urls.append(url)
            time.sleep(1)

        self.save_urls_to_cache(scraped_urls)
        return results
