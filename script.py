import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
from delete_data import filter_by_date
from urls import urls
import logging
import os
from datetime import datetime
import time  # NEW: For rate limiting
from requests.adapters import HTTPAdapter  # NEW: For connection retries
from urllib3.util.retry import Retry  # NEW: For retry strategy

print("start")

# Create logs directory if it doesn't exist
if not os.path.exists("logs"):
    os.makedirs("logs")

# Configure logging
log_filename = f"logs/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
print("log file created")

# NEW: Create a session with retry capabilities
def create_session():
    """Create a resilient HTTP session with retries and headers"""
    session = requests.Session()
    
    # Configure retry strategy (NEW)
    retry_strategy = Retry(
        total=3,  # Maximum 3 retries
        backoff_factor=1,  # Delay between retries: 1s, 2s, 4s etc.
        status_forcelist=[500, 502, 503, 504],  # Retry on these status codes
        allowed_methods=["GET"]  # Only retry safe GET requests
    )
    
    # Mount retry adapter (NEW)
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    # Set default headers (NEW: Mimic browser request)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })
    
    return session

def scrape_data(url):
    """
    Scrapes a table from a given URL, extracts all links from the last column,
    and returns the data as a Pandas DataFrame.
    """
    try:
        # NEW: Use session instead of direct requests.get
        session = create_session()
        
        # ORIGINAL: response = requests.get(url)
        response = session.get(url, timeout=10)  # NEW: Added timeout
        response.raise_for_status()
        print("data scrapped...")

        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table")
        print("table found...")

        if table is None:
            logging.warning(f"No table found on the given URL: {url}")
            return None

        headers = [th.text.strip() for th in table.find_all("th")]
        headers.insert(0, "District")  # Add "District" to headers

        data = []
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            row_data = [col.text.strip() for col in cols]

            # Extract all links from the last column and join them
            if cols:
                last_col = cols[-1]
                link_tags = last_col.find_all("a")
                links = [urljoin(url, link_tag["href"]) for link_tag in link_tags if "href" in link_tag.attrs]
                row_data[-1] = "; ".join(links)  # Join with semicolon
            data.append(row_data)
        print("table created and data stored...")    

        # Get district name and add to each row
        # NEW: Added try-except for URL parsing
        try:
            district_name = url.split(".")[0].split("//")[1].capitalize()
        except IndexError as e:
            logging.error(f"URL format error: {url} - {str(e)}")
            district_name = "Unknown"
            
        for row in data:
            row.insert(0, district_name)

        df = pd.DataFrame(data, columns=headers)
        print(f"Table data from {url}")
        logging.info(f"Table data from {url}")
        return df

    except requests.exceptions.RequestException as e:
        # NEW: Added detailed error logging
        print(f"Error during request: {e}")
        logging.error(f"Request failed for {url}: {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error at {url}: {str(e)}")
        return None

def create_json(data, json_filename):
    print("creating json file...")
    """
    Creates a JSON file from a Pandas DataFrame.
    """
    if data:
        try:  # NEW: Added error handling
            final_df = pd.DataFrame(data)
            final_df.to_json(json_filename, orient="records", indent=4)
            print(f"Combined data saved to {json_filename}")
            logging.info(f"Saved {len(final_df)} records to {json_filename}")
        except Exception as e:
            logging.error(f"JSON creation failed: {str(e)}")
    else:
        logging.warning("No data to save to JSON.")

# Main execution with rate limiting (NEW)
if __name__ == "__main__":
    all_data = []
    print("ready to parse urls")
    
    # NEW: Create persistent session
    session = create_session()
    
    for index, url in enumerate(urls):
        # NEW: Add delay between requests (2 seconds)
        if index > 0:
            time.sleep(2)
            
        df = scrape_data(url)
        if df is not None:
            all_data.append(df)
    
    print("data collected")
    
    if all_data:  # NEW: Check if any data exists
        try:
            combined_df = pd.concat(all_data, ignore_index=True)
            filtered_data = filter_by_date(combined_df.to_dict('records'))
            print("data filtered")
            create_json(filtered_data, "tenders.json")
        except Exception as e:
            logging.error(f"Data processing failed: {str(e)}")
    else:
        logging.warning("No data collected from any URLs")
