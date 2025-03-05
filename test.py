print("start")
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
from delete_data import filter_by_date
from urls import urls
import logging
import os
from datetime import datetime

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
