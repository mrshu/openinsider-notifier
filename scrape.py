import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
import logging
import hashlib
import asyncio
from matrix_send import matrix_send

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Function to scrape the table and convert it into a DataFrame
def scrape_table(url, table_class="tinytable"):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        soup = BeautifulSoup(response.content, "html.parser")

        # Find table by class
        table = soup.find("table", class_=table_class)
        if not table:
            logging.error(f"No table found with class: {table_class}")
            return pd.DataFrame()

        # Extract headers and rows
        headers = [header.text.strip() for header in table.find_all("th")]
        rows = [
            [col.text.strip() for col in row.find_all("td")]
            for row in table.find_all("tr")[1:]  # Skip the header row
        ]

        if not rows:
            logging.warning("No rows found in the table.")

        return pd.DataFrame(rows, columns=headers)

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching the URL: {e}")
        return pd.DataFrame()


# Function to read the saved JSONL file into a DataFrame
def read_jsonl_file(filepath="trades.jsonl"):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                data = [json.loads(line) for line in f]
            return pd.DataFrame(data)
        except (IOError, json.JSONDecodeError) as e:
            logging.error(f"Error reading JSONL file: {e}")
            return pd.DataFrame()
    else:
        logging.info(f"File {filepath} not found. Starting with an empty dataset.")
        return pd.DataFrame()


def save_to_jsonl(df, filepath="trades.jsonl"):
    with open(filepath, "a") as f:
        for _, row in df.iterrows():
            f.write(json.dumps(row.to_dict()) + "\n")


# Function to hash each row of a DataFrame to detect new data more efficiently
def hash_row(row):
    row_string = "".join(map(str, row))
    return hashlib.md5(row_string.encode()).hexdigest()


# Function to compare two DataFrames and return new rows
def get_new_additions(scraped_df, saved_df):
    if saved_df.empty:
        logging.info("No existing data found, treating all scraped data as new.")
        return scraped_df

    # Hash rows for comparison
    scraped_hashes = set(scraped_df.apply(hash_row, axis=1))
    saved_hashes = set(saved_df.apply(hash_row, axis=1))

    # Find new rows
    new_hashes = scraped_hashes - saved_hashes
    new_data = scraped_df[scraped_df.apply(hash_row, axis=1).isin(new_hashes)]

    return new_data


# Function to format each row for sending via a message service
def format_row_for_message(row):
    msg = "\n".join([f"{col}: {val}" for col, val in row.items()])

    msg += "\n\n"
    msg += f"https://www.openinsider.com/{row['Ticker']}\n"
    msg += f"https://finance.yahoo.com/quote/{row['Ticker']}\n"
    msg += f"https://finviz.com/quote.ashx?t={row['Ticker']}\n"

    return msg


# Main function to scrape, compare, and extend the data
def main(url):
    logging.info("Starting the data scraping and comparison process.")

    # Step 1: Scrape the current data from the table
    scraped_df = scrape_table(url)
    if scraped_df.empty:
        logging.error("Failed to scrape any data.")
        return

    # Step 2: Read the saved data from 'trades.jsonl'
    saved_df = read_jsonl_file()

    # Step 3: Compare scraped data with saved data
    new_additions = get_new_additions(scraped_df, saved_df)

    # Step 4: If new additions are found, notify and extend the saved file
    if not new_additions.empty:
        logging.info(f"Found {len(new_additions)} new additions.")

        for _, row in new_additions.iterrows():
            message = format_row_for_message(row)
            logging.info(f"""MSG: {message}""")
            asyncio.get_event_loop().run_until_complete(matrix_send(message))

        save_to_jsonl(new_additions)
    else:
        logging.info("No new additions found.")


# Example usage
if __name__ == "__main__":
    url = "http://www.openinsider.com/screener?s=&o=&pl=50&ph=&ll=&lh=&fd=90&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&vl=100&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&isofficer=1&iscob=1&isceo=1&ispres=1&iscoo=1&iscfo=1&isgc=1&isvp=1&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=500&page=1"  # noqa
    main(url)
