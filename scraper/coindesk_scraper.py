import logging
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from common.db import PostgresDB
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BASE_URL = "https://www.coindesk.com"
TARGET_URL = f"{BASE_URL}/price/bitcoin/news"
HEADERS = {"User-Agent": "Mozilla/5.0"}


class CoindeskScraper:
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
        rows = self.db.execute("SELECT url FROM cache_coindesk")
        return set(row["url"] for row in rows) if rows else set()

    def save_urls_to_cache(self, urls):
        for url in urls:
            self.db.execute(f"""
                INSERT INTO cache_coindesk (url) VALUES ('{url}')
                ON CONFLICT DO NOTHING
            """)

    def scrape_article_links(self):
        driver = self.init_driver()
        driver.get(TARGET_URL)

        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.relative"))
            )

            a_tags = driver.find_elements(By.CSS_SELECTOR, "a.relative")
            links = list({
                a.get_attribute("href")
                for a in a_tags
                if a.get_attribute("href")
            })

        except Exception as e:
            logging.error(f"No article links found: {e}")
            links = []
        finally:
            driver.quit()

        logging.info(f"Found {len(links)} article links")
        return links

    def parse_article(self, url):
        try:
            logging.info(f"Parsing {url}")
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")

            title_tag = soup.find("h1", class_="font-headline-lg")
            title = title_tag.get_text(strip=True) if title_tag else "No title"

            meta_date = soup.find("meta", attrs={"name": "create_date"})
            meta_time = soup.find("meta", attrs={"name": "create_time"})

            timestamp = None
            if meta_date and meta_time:
                dt_str = f"{meta_date['content']} {meta_time['content']}"
                dt = datetime.strptime(dt_str, "%Y%m%d %H:%M")
                timestamp = int(dt.timestamp())

            content_div = soup.find("div", attrs={"data-module-name": "article-body"})
            content = ""
            if content_div:
                paragraphs = content_div.find_all("p")
                content = "\n".join(p.get_text(strip=True) for p in paragraphs)

            return {
                "url": url,
                "title": title,
                "timestamp": timestamp,
                "content": content
            }

        except Exception as e:
            logging.error(f"Error scraping article {url}: {e}")
            return None

    def run(self, max_articles=5):
        all_links = self.scrape_article_links()
        cached = self.get_cached_urls()
        new_links = [url for url in all_links if url not in cached]

        logging.info(f"🆕 {len(new_links)} new articles to scrape")

        results = []
        scraped_urls = []
        for url in new_links:
            article = self.parse_article(url)
            if article:
                results.append(article)
                scraped_urls.append(url)
            time.sleep(1)

        results.sort(key=lambda x: x["timestamp"], reverse=True)
        self.save_urls_to_cache(scraped_urls)
        return results[:max_articles]
