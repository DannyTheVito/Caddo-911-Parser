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
FETCH_INTERVAL_SECONDS = 35
REPEAT_CALL_INTERVAL_SECONDS = 23 * 3600  # 23 hours


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

        # Exclude 'units' from the hash
        combined = ''.join([event[h] for h in HEADERS if h != "units"])
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
            FirstSeen DATETIME NOT NULL,
            LastSeen DATETIME NOT NULL,
            Resolved TINYINT(1) DEFAULT 0,
            KEY idx_agency (Agency) KEY_BLOCK_SIZE=1024,
            KEY idx_municipal (Municipal) KEY_BLOCK_SIZE=1024,
            FULLTEXT KEY idx_description (Description) KEY_BLOCK_SIZE=1024,
            FULLTEXT KEY idx_street_cross (Street, CrossStreets) KEY_BLOCK_SIZE=1024
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin KEY_BLOCK_SIZE=1
    """)


def get_event_last_seen(cursor, table_name, event_hash):
    cursor.execute(f"SELECT LastSeen FROM {table_name} WHERE Hash = %s ORDER BY LastSeen DESC LIMIT 1", (event_hash,))
    row = cursor.fetchone()
    return row[0] if row else None


def get_event_units(cursor, table_name, event_hash):
    cursor.execute(f"SELECT Units FROM {table_name} WHERE Hash = %s ORDER BY LastSeen DESC LIMIT 1", (event_hash,))
    row = cursor.fetchone()
    return row[0] if row else None


def insert_event(cursor, table_name, event):
    now_utc = datetime.datetime.utcnow()
    query = f"""
        INSERT INTO {table_name} 
        (Agency, Time, Units, Description, Street, CrossStreets, Municipal, Date, Hash, FirstSeen, LastSeen)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        event['agency'], event['time'], event['units'],
        event['description'], event['street'], event['cross_streets'],
        event['municipal'], now_utc, event['hash'], now_utc, now_utc
    )
    cursor.execute(query, params)


def update_last_seen(cursor, table_name, event_hash):
    now_utc = datetime.datetime.utcnow()
    cursor.execute(
        f"UPDATE {table_name} SET LastSeen = %s WHERE Hash = %s",
        (now_utc, event_hash)
    )


def update_units_and_last_seen(cursor, table_name, event_hash, new_units):
    now_utc = datetime.datetime.utcnow()
    cursor.execute(
        f"UPDATE {table_name} SET Units = %s, LastSeen = %s WHERE Hash = %s",
        (int(new_units), now_utc, event_hash)
    )


def mark_resolved_events(cursor, table_name, visible_hashes):
    cursor.execute(f"SELECT Hash FROM {table_name} WHERE Resolved = 0")
    active_hashes = cursor.fetchall()

    for (db_hash,) in active_hashes:
        if db_hash not in visible_hashes:
            cursor.execute(
                f"UPDATE {table_name} SET Resolved = 1 WHERE Hash = %s",
                (db_hash,)
            )


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
        visible_hashes = set(event['hash'] for event in events)

        try:
            with connect_db() as conn:
                with conn.cursor() as cursor:
                    for event in events:
                        table_name = sanitize_table_name(event['agency'])
                        create_agency_table(cursor, table_name)

                        last_seen = get_event_last_seen(cursor, table_name, event['hash'])

                        insert = False
                        if last_seen is None:
                            insert = True
                        else:
                            delta = datetime.datetime.utcnow() - last_seen
                            if delta.total_seconds() >= REPEAT_CALL_INTERVAL_SECONDS:
                                insert = True

                        if insert:
                            insert_event(cursor, table_name, event)
                            new_calls += 1
                        else:
                            existing_units = get_event_units(cursor, table_name, event['hash'])
                            if existing_units != int(event['units']):
                                update_units_and_last_seen(cursor, table_name, event['hash'], event['units'])
                            else:
                                update_last_seen(cursor, table_name, event['hash'])

                    # After processing all events, mark any missing ones as resolved
                    for agency in set(event['agency'] for event in events):
                        table_name = sanitize_table_name(agency)
                        mark_resolved_events(cursor, table_name, visible_hashes)

                conn.commit()
            logging.info(f"New calls added: {new_calls}")

        except mysql.connector.Error as err:
            logging.error(f"Database error: {err}")

        time.sleep(FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
