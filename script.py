import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
from delete_data import filter_by_date
from urls import urls
import logging
import os
import time
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
if not os.path.exists("logs"):
    os.makedirs("logs")

log_filename = f"logs/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def create_session():
    """Create HTTP session with retries and headers"""
    session = requests.Session()
    
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session

def scrape_data(url, session):
    """Scrape table data from URL with error handling"""
    try:
        start_time = time.time()
        logging.info(f"Scraping: {url}")
        
        response = session.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table")
        
        if not table:
            logging.warning(f"No table found at {url}")
            return None

        # Data extraction logic
        headers = [th.text.strip() for th in table.find_all("th")]
        headers.insert(0, "District")
        
        data = []
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            row_data = [col.text.strip() for col in cols]
            
            if cols:
                last_col = cols[-1]
                links = [
                    urljoin(url, link["href"]) 
                    for link in last_col.find_all("a", href=True)
                ]
                row_data[-1] = "; ".join(links)
                
            data.append(row_data)
        
        # Add district name
        district = url.split("//")[1].split(".")[0].capitalize()
        for row in data:
            row.insert(0, district)
            
        logging.info(f"Scraped {url} in {time.time()-start_time:.2f}s")
        return pd.DataFrame(data, columns=headers)
        
    except Exception as e:
        logging.error(f"Failed {url}: {str(e)}")
        return None

def create_json(data, filename="tenders.json"):
    """Save filtered data to JSON"""
    if not data:
        logging.warning("No data to save")
        return
    
    try:
        df = pd.DataFrame(data)
        df.to_json(filename, orient="records", indent=4)
        logging.info(f"Saved {len(df)} records to {filename}")
    except Exception as e:
        logging.error(f"Failed to save JSON: {str(e)}")

if __name__ == "__main__":
    session = create_session()
    all_data = []
    
    for idx, url in enumerate(urls):
        if idx > 0:
            time.sleep(2)  # Rate limiting
            
        df = scrape_data(url, session)
        if df is not None:
            all_data.append(df)
    
    filtered = filter_by_date(pd.concat(all_data).to_dict("records"))
    create_json(filtered)
