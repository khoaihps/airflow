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

BASE_URL = "https://cointelegraph.com"
TARGET_URL = f"{BASE_URL}/bitcoin-price"
HEADERS = {"User-Agent": "Mozilla/5.0"}


class CointelegraphScraper:
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
        rows = self.db.execute("SELECT url FROM cache_cointelegraph")
        return set(row["url"] for row in rows) if rows else set()

    def save_urls_to_cache(self, urls):
        for url in urls:
            self.db.execute(f"""
                INSERT INTO cache_cointelegraph (url) VALUES ('{url}')
                ON CONFLICT DO NOTHING
            """)

    def scrape_article_links(self):
        driver = self.init_driver()
        driver.get(TARGET_URL)

        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'article[data-testid="article-card article-card-inline"]'))
            )

            article_cards = driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="article-card article-card-inline"]')

            links = list({
                a.get_attribute("href")
                for article in article_cards
                for a in article.find_elements(By.TAG_NAME, "a")[:1]
                if a.get_attribute("href") and a.get_attribute("href").startswith("https://cointelegraph.com")
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

            title_tag = soup.find("h1")
            title = title_tag.get_text(strip=True) if title_tag else "No title"

            time_tag = soup.find("time")
            timestamp = None
            if time_tag and time_tag.has_attr("datetime"):
                dt = datetime.fromisoformat(time_tag["datetime"].replace("Z", "+00:00"))
                timestamp = int(dt.timestamp())

            content_div = soup.find("div", class_="post-content")
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

        results.sort(key=lambda x: x["timestamp"] or 0, reverse=True)
        self.save_urls_to_cache(scraped_urls)
        return results[:max_articles]
