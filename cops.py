import hashlib
import time
import datetime
import requests
from bs4 import BeautifulSoup
import mysql.connector
import config
import logging
import re
from thefuzz import fuzz
from datetime import UTC

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
MATCH_THRESHOLD = 75

GEO_CACHE = {}

# GEOLOCATION HELPERS

def get_anchor(text):
    if not text: return None
    noise = r'\b(NORTH|SOUTH|EAST|WEST|N|S|E|W|RD|ST|AVE|AV|BLVD|LP|LOOP|PKWY|HWY|DR|INDUSTRIAL|BLOCK|BLK|III|DEAD|END)\b'
    words = re.sub(noise, '', text.upper(), flags=re.IGNORECASE).split()
    return max(words, key=len) if words else None

def find_node(cursor, st1, st2):
    if not st1 or not st2: return None
    key = tuple(sorted([st1.upper(), st2.upper()]))
    if key in GEO_CACHE: return GEO_CACHE[key]

    for search_term in [st1, st2]:
        anchor = get_anchor(search_term)
        if not anchor: continue
        cursor.execute("SELECT street_a, street_b, lat, lon FROM osm_intersections WHERE street_a LIKE %s OR street_b LIKE %s LIMIT 200", (f"%{anchor}%", f"%{anchor}%"))
        candidates = cursor.fetchall()
        best_match, best_score = None, 0
        for cand in candidates:
            s1 = fuzz.token_set_ratio(st1.upper(), cand['street_a'])
            s2 = fuzz.token_set_ratio(st2.upper(), cand['street_b'])
            s1r = fuzz.token_set_ratio(st1.upper(), cand['street_b'])
            s2r = fuzz.token_set_ratio(st2.upper(), cand['street_a'])
            score = max((s1 + s2) / 2, (s1r + s2r) / 2)
            if score > best_score:
                best_score, best_match = score, cand
        if best_match and best_score >= MATCH_THRESHOLD:
            GEO_CACHE[key] = {'lat': best_match['lat'], 'lon': best_match['lon']}
            return GEO_CACHE[key]
    return None

def geocode_call(cursor, street, cross_streets):
    st = (street or "").upper().strip()
    parts = [p.strip() for p in re.split(r' [&/] | AND ', (cross_streets or "").upper()) if p.strip()]
    if st and len(parts) >= 1:
        if len(parts) >= 2:
            n1, n2 = find_node(cursor, st, parts[0]), find_node(cursor, st, parts[1])
            if n1 and n2: return (n1['lat'] + n2['lat'])/2, (n1['lon'] + n2['lon'])/2
            elif n1 or n2:
                m = n1 if n1 else n2
                return m['lat'], m['lon']
        else:
            m = find_node(cursor, st, parts[0])
            if m: return m['lat'], m['lon']
    elif not st and len(parts) >= 2:
        m = find_node(cursor, parts[0], parts[1])
        if m: return m['lat'], m['lon']
    return None, None

# SCRAPER & DB CORE

def fetch_active_calls(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.content
    except Exception as e:
        logging.warning(f"Fetch failed: {e}")
        return None

def parse_calls(page_content):
    events = []
    soup = BeautifulSoup(page_content, 'html.parser')
    table = soup.find(id="ctl00_MainContent_GV_AE_ALL_P")
    if not table: return []
    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        row_data = [cell.text.strip().replace("\xa0", "") if cell.contents else "" for cell in cells]
        if len(row_data) != len(HEADERS): continue
        event = dict(zip(HEADERS, row_data))
        event['hash'] = hashlib.md5(''.join([event[h] for h in HEADERS if h != 'units']).encode('utf-8')).hexdigest()
        events.append(event)
    return events

def create_agency_table(cursor, table_name):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Agency VARCHAR(10) NOT NULL,
            Time VARCHAR(4) NOT NULL,
            Units SMALLINT NOT NULL,
            Description VARCHAR(255),
            Street VARCHAR(255),
            CrossStreets VARCHAR(255),
            Municipal VARCHAR(10),
            Date DATETIME DEFAULT NULL,
            Hash VARCHAR(64) NOT NULL,
            FirstSeen DATETIME NOT NULL,
            LastSeen DATETIME NOT NULL,
            Resolved TINYINT(1) DEFAULT 0,
            lat DECIMAL(10, 8) NULL,
            lon DECIMAL(11, 8) NULL,
            UNIQUE KEY unique_event (Hash, FirstSeen),
            INDEX (Hash)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
    """)

def insert_event(cursor, table_name, event):
    lat, lon = geocode_call(cursor, event['street'], event['cross_streets'])
    now_utc = datetime.datetime.now(UTC).replace(tzinfo=None)
    query = f"""
        INSERT INTO {table_name} 
        (Agency, Time, Units, Description, Street, CrossStreets, Municipal, Date, Hash, FirstSeen, LastSeen, lat, lon)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (event['agency'], event['time'], event['units'], event['description'], 
              event['street'], event['cross_streets'], event['municipal'], 
              now_utc, event['hash'], now_utc, now_utc, lat, lon)
    cursor.execute(query, params)
    return lat is not None

def mark_resolved_events(cursor, table_name, current_hashes):
    cursor.execute(f"SELECT Hash, Resolved, FirstSeen FROM {table_name} WHERE Resolved = 0")
    all_rows = cursor.fetchall()
    marked_resolved = 0
    now = datetime.datetime.now(UTC).replace(tzinfo=None)
    for row in all_rows:
        first_seen = row['FirstSeen']
        if isinstance(first_seen, str):
            first_seen = datetime.datetime.strptime(first_seen, '%Y-%m-%d %H:%M:%S')
        
        age = now - first_seen
        if (row['Hash'] not in current_hashes or age.total_seconds() > REINSERT_THRESHOLD_HOURS * 3600):
            cursor.execute(f"UPDATE {table_name} SET Resolved = 1 WHERE Hash = %s", (row['Hash'],))
            marked_resolved += 1
    return marked_resolved

def main():
    logging.info("Caddo Active Calls Monitor. Now With Geolocating!")
    while True:
        start_time = time.time()
        
        page_content = fetch_active_calls(URL)
        if not page_content:
            time.sleep(FETCH_INTERVAL_SECONDS)
            continue

        events = parse_calls(page_content)
        visible_hashes_by_agency = {}
        
        stats = {
            'new': 0,
            'geocoded': 0,
            'updated': 0,
            'resolved': 0,
            'total_open': 0
        }

        try:
            with mysql.connector.connect(**DB_CONFIG) as conn:
                with conn.cursor(dictionary=True) as cursor:
                    created_tables = set()

                    for event in events:
                        t_name = "agency_" + re.sub(r'\W+', '', event['agency'][:3])
                        if t_name not in created_tables:
                            create_agency_table(cursor, t_name)
                            created_tables.add(t_name)

                        if t_name not in visible_hashes_by_agency:
                            visible_hashes_by_agency[t_name] = set()
                        visible_hashes_by_agency[t_name].add(event['hash'])

                        cursor.execute(f"SELECT id FROM {t_name} WHERE Hash = %s ORDER BY FirstSeen DESC LIMIT 1", (event['hash'],))
                        if not cursor.fetchone():
                            was_geocoded = insert_event(cursor, t_name, event)
                            stats['new'] += 1
                            if was_geocoded: stats['geocoded'] += 1
                        else:
                            cursor.execute(f"UPDATE {t_name} SET Units = %s, LastSeen = %s WHERE Hash = %s ORDER BY FirstSeen DESC LIMIT 1",
                                           (event['units'], datetime.datetime.now(UTC).replace(tzinfo=None), event['hash']))
                            stats['updated'] += 1

                    # Resolve calls & Count Open Calls
                    cursor.execute("SHOW TABLES LIKE 'agency_%'")
                    all_tables = [list(row.values())[0] for row in cursor.fetchall()]
                    
                    for t in all_tables:
                        stats['resolved'] += mark_resolved_events(cursor, t, visible_hashes_by_agency.get(t, set()))
                        cursor.execute(f"SELECT COUNT(*) as active FROM {t} WHERE Resolved = 0")
                        stats['total_open'] += cursor.fetchone()['active']
                    
                    conn.commit()

            processing_time = time.time() - start_time

            logging.info("┌────────────────────────────────────┐")
            logging.info(f"│ SCRAPE SUMMARY - {datetime.datetime.now().strftime('%H:%M:%S')}      │")
            logging.info("├────────────────────────────────────┤")
            logging.info(f"│ New Calls Added:      {stats['new']:>12} │")
            logging.info(f"│ Successfully Geocoded: {stats['geocoded']:>12} │")
            logging.info(f"│ Updated Units/Time:   {stats['updated']:>12} │")
            logging.info(f"│ Marked Resolved:      {stats['resolved']:>12} │")
            logging.info("├────────────────────────────────────┤")
            logging.info(f"│ TOTAL OPEN CALLS:     {stats['total_open']:>12} │")
            logging.info(f"│ Processing Time:      {processing_time:>11.2f}s │")
            logging.info("└────────────────────────────────────┘")

        except mysql.connector.Error as err:
            logging.error(f"Database error: {err}")

        time.sleep(FETCH_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
