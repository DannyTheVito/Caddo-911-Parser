import hashlib
import time
import datetime
import requests
from bs4 import BeautifulSoup
import mysql.connector
import config
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_CONFIG = {
    "host": config.databasehost,
    "user": config.databaseuser,
    "password": config.databasepasswd,
    "database": config.databasename,
    "auth_plugin": "caching_sha2_password"
}

URL = 'http://ias.ecc.caddo911.com/All_ActiveEvents.aspx'

HEADERS = ["agency", "time", "units", "description", "street", "cross_streets", "municipal"]

FETCH_INTERVAL_SECONDS = 180


def fetch_active_calls(url, retries=3, delay=60):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    for _ in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=8, allow_redirects=True)
            response.raise_for_status()
            logging.info(f"Final URL after redirects: {response.url}")
            return response.content
        except requests.exceptions.RequestException as e:
            logging.warning(f"Requests error: {e}")
            time.sleep(delay)
    return None


def parse_calls(page_content):
    events = []
    soup = BeautifulSoup(page_content, 'html.parser')
    table = soup.find(id="ctl00_MainContent_GV_AE_ALL_P")

    if not table:
        logging.warning("No event table found in HTML.")
        return []

    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        row_data = [cell.text.strip().replace("\xa0", "") if cell.contents else "" for cell in cells]
        if len(row_data) != len(HEADERS):
            continue
        event = dict(zip(HEADERS, row_data))

        combined = ''.join([event[h] for h in HEADERS])
        event['hash'] = hashlib.md5(combined.encode('utf-8')).hexdigest()

        events.append(event)

    return events


def connect_db():
    return mysql.connector.connect(**DB_CONFIG)


def sanitize_table_name(agency_code):
    return "agency_" + re.sub(r'\W+', '', agency_code[:3])


def create_agency_table(cursor, table_name):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Agency VARCHAR(10) COLLATE utf8_bin NOT NULL,
            Time VARCHAR(4) NOT NULL,
            Units SMALLINT NOT NULL,
            Description VARCHAR(255) COLLATE utf8_bin,
            Street VARCHAR(255) COLLATE utf8_bin,
            CrossStreets VARCHAR(255) COLLATE utf8_bin,
            Municipal VARCHAR(10) COLLATE utf8_bin,
            Date DATETIME DEFAULT NULL,
            Hash VARCHAR(64) COLLATE utf8_bin NOT NULL,
            FirstSeen DATETIME DEFAULT CURRENT_TIMESTAMP,
            LastSeen DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_event (Hash),
            KEY idx_agency (Agency) KEY_BLOCK_SIZE=1024,
            KEY idx_municipal (Municipal) KEY_BLOCK_SIZE=1024,
            FULLTEXT KEY idx_description (Description) KEY_BLOCK_SIZE=1024,
            FULLTEXT KEY idx_street_cross (Street, CrossStreets) KEY_BLOCK_SIZE=1024
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin KEY_BLOCK_SIZE=1
    """)


def upsert_event(cursor, table_name, event):
    query = f"""
        INSERT INTO {table_name} 
        (Agency, Time, Units, Description, Street, CrossStreets, Municipal, Date, Hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE LastSeen = CURRENT_TIMESTAMP
    """
    params = (
        event['agency'], event['time'], event['units'],
        event['description'], event['street'], event['cross_streets'],
        event['municipal'], datetime.datetime.now(), event['hash']
    )
    cursor.execute(query, params)


def main():
    logging.info("Starting Active Calls Monitor")

    while True:
        new_calls = 0
        page_content = fetch_active_calls(URL)

        if not page_content:
            logging.warning("No content fetched. Retrying after delay.")
            time.sleep(FETCH_INTERVAL_SECONDS)
            continue

        events = parse_calls(page_content)

        try:
            with connect_db() as conn:
                with conn.cursor() as cursor:
                    for event in events:
                        table_name = sanitize_table_name(event['agency'])
                        create_agency_table(cursor, table_name)

                        upsert_event(cursor, table_name, event)
                        new_calls += 1
                conn.commit()

            logging.info(f"Processed calls (new or updated): {new_calls}")
        except mysql.connector.Error as err:
            logging.error(f"Database error: {err}")

        time.sleep(FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
