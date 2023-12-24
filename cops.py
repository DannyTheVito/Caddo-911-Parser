from bs4 import BeautifulSoup
import hashlib
import urllib.request
import time
import datetime
from socket import timeout
import mysql.connector
import config


# Events get stored
events = []
new_calls = 0


while True:
  events = []
  # open the URL and retry if timeout
  while True:
    try:
      page_content = urllib.request.urlopen('http://ias.ecc.caddo911.com/All_ActiveEvents.aspx', timeout=8).read()
      break
    except urllib.error.URLError:
      print("URL Error")
      time.sleep(60)
    except timeout:
      print("Timed out")
      time.sleep(60)

  # This is an explanation of what occurs next.
  # numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9]
  # numbers[0]   -----> 1
  # numbers[5:]  -----> [6, 7, 8, 9]
  # numbers[:8]  -----> [1, 2, 3, 4, 5, 6, 7, 8]
  # numbers[5:8] -----> [6, 7, 8]

  page_data = BeautifulSoup(page_content, 'html.parser')
  table_data = page_data.find(id="ctl00_MainContent_GV_AE_ALL_P")

  # Parse the page
  for row in table_data.find_all("tr")[1:]:
    row_data = []

    for cell in row.find_all("td"):
      if len(cell.contents) > 0:
        row_data.append(cell.text)
      else:
        row_data.append("")

    row_data = [x.replace("\xa0", "").strip() for x in row_data]
    output = dict(zip(["agency", "time", "units", "description", "street", "cross_streets", "municipal"], row_data))
    events.append(output)

  # Make a hash to avoid duplicates in the database
  for event in events:
    combined_string = bytes(event['time'] + event['agency'] + event['description'] + event['street'] + event['cross_streets'] + event['municipal'], 'utf-8')
    hash = hashlib.md5(combined_string)
    event['hash'] = hash.hexdigest()
	

  conn = mysql.connector.connect(host=config.databasehost,
          user=config.databaseuser,
          passwd=config.databasepasswd,
          db=config.databasename,
	  auth_plugin='mysql_native_password')
  cursor = conn.cursor()
  for event in events:
    agency_table = "agency_{agency}".format(agency=event['agency'][:3])

    # Makes the agency table if it doesn't exist
    table_create =  ("""CREATE TABLE IF NOT EXISTS {} (
           `id` INT(11) NOT NULL PRIMARY KEY AUTO_INCREMENT,
           `Agency` VARCHAR(10) COLLATE utf8_bin NOT NULL,
           `Time` VARCHAR(4) NOT NULL,
           `Units` SMALLINT(3) NOT NULL,
           `Description` VARCHAR(255) COLLATE utf8_bin DEFAULT NULL,
           `Street` VARCHAR(255) COLLATE utf8_bin DEFAULT NULL,
           `CrossStreets` VARCHAR(255) COLLATE utf8_bin DEFAULT NULL,
           `Municipal` VARCHAR(10) COLLATE utf8_bin DEFAULT NULL,
           `Date` datetime DEFAULT NULL,
           `Hash` VARCHAR(64) COLLATE utf8_bin DEFAULT NULL,
            KEY `idx_base_agency` (`Agency`) KEY_BLOCK_SIZE=1024,
            KEY `idx_base_municipal` (`Municipal`) KEY_BLOCK_SIZE=1024,
            FULLTEXT KEY `idx_base_Description` (`Description`) KEY_BLOCK_SIZE=1024,
            FULLTEXT KEY `idx_base_Street_CrossStreets` (`Street`,`CrossStreets`) KEY_BLOCK_SIZE=1024
            ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin KEY_BLOCK_SIZE=1 """).format(agency_table)
    cursor.execute(table_create)

    # Checks if the same call exists 
    get_existing_query = (f'SELECT * FROM {agency_table} WHERE Hash="{event["hash"]}" AND Date >= now() - INTERVAL 23 HOUR')
    cursor.execute(get_existing_query)
    existing_rows = cursor.fetchall()

    call_exists = len(existing_rows) > 0

    # Inserts it into the database if new call
    if not call_exists:
      new_calls +=1
      table_insert = ("""INSERT INTO {} VALUES (null, %s, %s, %s, %s, %s, %s, %s, %s, %s )""").format(agency_table) 
      params = event['agency'], event['time'], event['units'], event['description'], event['street'], event['cross_streets'], event['municipal'], datetime.datetime.now(), event['hash']
      cursor.execute(table_insert, params)

  conn.commit()
  print("****************************")
  print("New calls added:", new_calls)
  print("Waiting 3 minutes before checking again")
  print("Current time:", time.ctime())
  print("****************************")
  time.sleep(3)
  cursor.close()
  conn.close()
  # Loop every 3 minutes
  time.sleep(60 * 3)
