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
FETCH_INTERVAL_SECONDS = 30
REINSERT_THRESHOLD_HOURS = 23

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

        combined = ''.join([event[h] for h in HEADERS if h != 'units'])
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
            UNIQUE KEY unique_event (Hash, FirstSeen),
            INDEX idx_agency (Agency),
            INDEX idx_municipal (Municipal),
            INDEX idx_hash_lastseen (Hash, LastSeen),
            FULLTEXT INDEX idx_description (Description),
            FULLTEXT INDEX idx_street_cross (Street, CrossStreets)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
    """)

def get_latest_event_by_hash(cursor, table_name, event_hash):
    cursor.execute(f"SELECT id, FirstSeen FROM {table_name} WHERE Hash = %s ORDER BY FirstSeen DESC LIMIT 1", (event_hash,))
    return cursor.fetchone()

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

def update_units_and_last_seen(cursor, table_name, event_hash, new_units):
    cursor.execute(
        f"""
        UPDATE {table_name}
        SET Units = %s, LastSeen = %s
        WHERE Hash = %s
        ORDER BY FirstSeen DESC
        LIMIT 1
        """,
        (new_units, datetime.datetime.utcnow(), event_hash)
    )

def mark_resolved_events(cursor, table_name, current_hashes):
    cursor.execute(f"SELECT Hash, Resolved, FirstSeen FROM {table_name} WHERE Resolved = 0")
    all_hashes = cursor.fetchall()

    marked_resolved = 0
    marked_unresolved = 0
    now = datetime.datetime.utcnow()

    for db_hash, resolved_flag, first_seen in all_hashes:
        age = now - first_seen
        if (db_hash not in current_hashes or age.total_seconds() > REINSERT_THRESHOLD_HOURS * 3600) and resolved_flag == 0:
            cursor.execute(f"UPDATE {table_name} SET Resolved = 1 WHERE Hash = %s", (db_hash,))
            marked_resolved += 1
          
    return marked_resolved, marked_unresolved

def main():
    logging.info("Starting Active Calls Monitor")

    while True:
        new_calls = 0
        updated_calls = 0
        total_resolved = 0
        total_unresolved = 0

        page_content = fetch_active_calls(URL)

        if not page_content:
            logging.warning("No content fetched. Retrying after delay.")
            time.sleep(FETCH_INTERVAL_SECONDS)
            continue

        events = parse_calls(page_content)
        visible_hashes_by_agency = {}

        try:
            with connect_db() as conn:
                with conn.cursor() as cursor:
                    created_tables = set()

                    for event in events:
                        table_name = sanitize_table_name(event['agency'])

                        if table_name not in created_tables:
                            create_agency_table(cursor, table_name)
                            created_tables.add(table_name)

                        if table_name not in visible_hashes_by_agency:
                            visible_hashes_by_agency[table_name] = set()
                        visible_hashes_by_agency[table_name].add(event['hash'])

                        latest_event = get_latest_event_by_hash(cursor, table_name, event['hash'])
                        now = datetime.datetime.utcnow()

                        if not latest_event:
                            insert_event(cursor, table_name, event)
                            new_calls += 1
                        else:
                            latest_id, first_seen = latest_event
                            age = now - first_seen
                            if age.total_seconds() > REINSERT_THRESHOLD_HOURS * 3600:
                                insert_event(cursor, table_name, event)
                                new_calls += 1
                            else:
                                update_units_and_last_seen(cursor, table_name, event['hash'], event['units'])
                                updated_calls += 1

                    cursor.execute("SHOW TABLES LIKE 'agency_%'")
                    all_tables = [row[0] for row in cursor.fetchall()]

                    for table_name in all_tables:
                        current_hashes = visible_hashes_by_agency.get(table_name, set())
                        marked_resolved, marked_unresolved = mark_resolved_events(cursor, table_name, current_hashes)
                        total_resolved += marked_resolved
                        total_unresolved += marked_unresolved

                conn.commit()

            logging.info("========== Scrape Summary ==========")
            logging.info(f"New Calls:          {new_calls:>4}")
            logging.info(f"Current Calls:      {updated_calls:>4}")
            logging.info(f"Resolved Calls:     {total_resolved:>4}")
            logging.info("====================================")
        except mysql.connector.Error as err:
            logging.error(f"Database error: {err}")

        time.sleep(FETCH_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
