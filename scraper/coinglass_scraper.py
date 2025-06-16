import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class CoinglassScraper:
    def __init__(self, url: str):
        self.url = url

    def init_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        return webdriver.Remote(
            command_executor="http://selenium:4444/wd/hub",
            options=options
        )

    def parse_value(self, raw_value: str) -> float:
        try:
            val = raw_value.replace("$", "").upper()
            if val.endswith("M"):
                return float(val[:-1]) * 1_000_000
            elif val.endswith("K"):
                return float(val[:-1]) * 1_000
            else:
                return float(val)
        except Exception as e:
            logging.warning(f"⚠️ Cannot parse value: {raw_value} → {e}")
            return 0.0

    def scrape(self) -> dict:
        driver = self.init_driver()
        driver.get(self.url)

        try:
            row = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'tr[data-row-key="BTC"]'))
            )
            tds = row.find_elements(By.TAG_NAME, 'td')

            if len(tds) < 6:
                logging.warning("Không đủ số lượng <td> để extract dữ liệu.")
                return {}

            return {
                "long": self.parse_value(tds[-6].text.strip()),
                "short": self.parse_value(tds[-5].text.strip()),
            }

        except Exception as e:
            logging.error(f"❌ Error scraping {self.url}: {e}")
            return {}
        finally:
            driver.quit()
