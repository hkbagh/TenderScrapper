import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
from delete_data import filter_by_date
from urls import urls
import logging
import os
from datetime import datetime
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
def scrape_data(url):
    """
    Scrapes a table from a given URL, extracts all links from the last column,
    and returns the data as a Pandas DataFrame.

    Args:
        url (str): The URL of the webpage containing the table.

    Returns:
        pd.DataFrame: The scraped data as a DataFrame, or None if an error occurred.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table")

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

        # Get district name and add to each row
        district_name = url.split(".")[0].split("//")[1].capitalize()
        for row in data:
            row.insert(0, district_name)

        df = pd.DataFrame(data, columns=headers)
        print(f"Table data from {url}")
        logging.info(f"Table data from {url}")
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")
        logging.error(f"Error during request: {e}")
        return None
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None


def create_json(data, json_filename):
    """
    Creates a JSON file from a Pandas DataFrame.

    Args:
        data (pd.DataFrame): The DataFrame to be saved.
        json_filename (str): The desired filename for the JSON output.
    """
    if data:
        # Convert list of dicts (from filtering) to DataFrame
        final_df = pd.DataFrame(data)
        final_df.to_json(json_filename, orient="records", indent=4)
        print(f"Combined data saved to {json_filename}")
        logging.info(f"Combined data saved to {json_filename}")
    else:
        logging.warning("No data to save to JSON.")


# Example usage
# urls = ['https://sundargarh.odisha.gov.in/tender'] # remove the hardcoded list

all_data = []
for url in urls:
    df = scrape_data(url)
    if df is not None:
        all_data.append(df)

# Filter the data *before* creating the JSON
filtered_data = filter_by_date(pd.concat(all_data, ignore_index=True).to_dict('records'))

create_json(filtered_data, "tenders.json")
