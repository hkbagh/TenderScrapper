import pandas as pd
from datetime import datetime

def filter_by_date(data, today_date_str=None): # todaay_date_str expects the format "YYYY-MM-DD"
    """
    Filters a list of dictionaries, keeping only entries where the 'End Date'
    is greater than today's date.  Assumes 'End Date' is in 'DD-MM-YYYY' format.

    Args:
        data (list of dict):  The scraped data, where each item is a dictionary
                              representing a row from the table.  Each dictionary
                              is expected to have an "End Date" key.
        today_date_str (str, optional): Today's date in 'YYYY-MM-DD' format.
                                        If None, defaults to the current system date.

    Returns:
        list of dict: Filtered data with 'End Date' greater than today.
                       Returns an empty list if input data is None or empty.
    """
    if not data:
        return []

    filtered_data = []
    if today_date_str:
        try:
            today = datetime.strptime(today_date_str, "%Y-%m-%d")
        except ValueError:
            print("Invalid date format for today's date. Please use YYYY-MM-DD.")
            return []  # Or raise the exception, depending on desired behavior
    else:
        today = datetime.today()
    for row in data:
        try:
            end_date_str = row.get("End Date", "").split(" -")[0]
            end_date = datetime.strptime(end_date_str, "%d-%m-%Y")
            if end_date > today:
                filtered_data.append(row)
        except (ValueError, TypeError):
            # Handle cases where date parsing fails or "End Date" is missing.
            #  ValueError for incorrect format, TypeError if end_date_str is None
            print(f"Skipping row due to invalid date format or missing End Date: {row}")
            continue  

    return filtered_data