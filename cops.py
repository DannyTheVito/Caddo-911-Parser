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


while True:
  events = []
  # open the URL and retry if timeout
  while True:
    try:
      page_content = urllib.request.urlopen('http://ias.ecc.caddo911.com/ActiveEvents.asp', timeout=8).read()
      break
    except urllib.error.URLError:
      print("URL Error")
      time.sleep(60)
    except timeout:
      print("Timed out")
      time.sleep(60)
  
  # Use HTML comments to trim page down

  start = page_content.find(b"<!--maincontent-->")
  end = page_content.find(b"<!-- Start of HTML Footer -->")

  # This is an explanation of what occurs next.
  # numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9]
  # numbers[0]   -----> 1
  # numbers[5:]  -----> [6, 7, 8, 9]
  # numbers[:8]  -----> [1, 2, 3, 4, 5, 6, 7, 8]
  # numbers[5:8] -----> [6, 7, 8]

  table_data = page_content[start:end]
  table_data = BeautifulSoup(table_data)

  # Parse the page
  for row in table_data.find_all("tr")[1:]:
    row_data = []

    for cell in row.find_all("td"):
      if len(cell.contents) > 0:
        row_data.append(cell.contents[0])
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
          db=config.databasename)
  cursor = conn.cursor()
  for event in events:
    agency_test = "agency_{agency}".format(agency=event['agency'][:3])

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
           `Hash` VARCHAR(64) UNIQUE COLLATE utf8_bin DEFAULT NULL,
            KEY `idx_base_agency` (`Agency`) KEY_BLOCK_SIZE=1024,
            KEY `idx_base_municipal` (`Municipal`) KEY_BLOCK_SIZE=1024,
            FULLTEXT KEY `idx_base_Description` (`Description`) KEY_BLOCK_SIZE=1024,
            FULLTEXT KEY `idx_base_Street_CrossStreets` (`Street`,`CrossStreets`) KEY_BLOCK_SIZE=1024
            ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin KEY_BLOCK_SIZE=1 """).format(agency_test)
    cursor.execute(table_create)

    # Inserts it into the database
    table_insert = ("""INSERT IGNORE INTO {} VALUES (null, %s, %s, %s, %s, %s, %s, %s, %s, %s )""").format(agency_test) 
    params = event['agency'], event['time'], event['units'], event['description'], event['street'], event['cross_streets'], event['municipal'], datetime.datetime.now(), event['hash']
    cursor.execute(table_insert, params)

  conn.commit()
  print("Sleeping 5 seconds for commit")
  time.sleep(5)
  cursor.close()
  conn.close()
  # Check every 5 minutes
  print("Sleeping for 5 minutes")
  print(time.ctime())
  time.sleep(60 * 5)
