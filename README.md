# Caddo 911 Parser

**Caddo 911 Parser** is a Python script that monitors, stores, and geolocates active emergency calls from the Caddo 911 website. It parses data from a public webpage and logs structured call information into a MySQL database for long-term tracking, auditing, and analysis.

---

## 📌 Features

- Scrapes active emergency call data from `http://ias.ecc.caddo911.com/All_ActiveEvents.aspx`
- Extracts and stores data such as:
  - Agency
  - Time
  - Units dispatched
  - Description
  - Street and cross streets
  - Municipality
- Stores data in MySQL with per-agency tables
- Detects new, repeated, and updated calls
- Tracks call status with a `Resolved` flag for calls no longer shown
- Logs changes in unit count
- Avoids redundant entries unless 23 hours have passed
- Runs continuously with automatic retry on failure
- Geolocates calls using locally parsed intersection data from OpenStreetMap

---

## ⚙️ Configuration

Before running the script, make sure to define your database credentials in a `config.py` file:

```python
# config.py
databasehost = "your-db-host"
databaseuser = "your-db-username"
databasepasswd = "your-db-password"
databasename = "your-db-name"
```

# **Important** 
For geolocating to work, you must run the companion script at least once to populate the `osm_intersections` table in the database. This will download street data from OpenStreetMaps and parse intersections and street names. 
Do not run this more than once every 3 or so months. It completely wipes the table and repopulates it from a fresh download from OpenStreetMaps. This will be changed in future releases to append new street data. 

```bash
python FindStreets.py
```

---

## 🗃️ Database Schema

Each agency gets its own table named `agency_<AGC>`, where `<AGC>` is the first 3 characters of the sanitized agency name.

Table schema includes:

- `Agency`, `Time`, `Units`, `Description`, `Street`, `CrossStreets`, `Municipal`
- `Hash`: Unique fingerprint (excluding units)
- `FirstSeen`, `LastSeen`: UTC timestamps
- `Resolved`: Marks calls that have disappeared from the source site
- Full-text indexes on `Description`, `Street`, and `CrossStreets`
- `lat/lon`: latitude and longitude for geolocated calls

---

## 🔁 How It Works

1. Every **30 seconds**, the script fetches the active calls page.
2. Parses the HTML table of calls.
3. For each call:
   - A hash (excluding `Units`) is computed.
   - A geolocation is attempted to be found.
   - The corresponding agency table is created if missing.
   - The call is:
     - Inserted if it's new
     - Re-inserted if 23 hours have passed since last seen
     - Updated if `Units` changed
     - Otherwise, `LastSeen` is refreshed
4. Calls no longer present in the latest scrape are marked as `Resolved = 1`.

---

## ▶️ Running the Script

Ensure your environment has the required libraries:

```bash
pip install requests beautifulsoup4 mysql-connector-python thefuzz sqlalchemy osmnx pandas
```

Then simply run:

```bash
python cops.py
```

---

## 📝 Notes

- Designed for long-term unattended operation.
- Logs activity to the console for monitoring.
- Can be deployed as a service or cron-managed background job.

---

## 📄 License

This script is provided as-is, without warranty or guarantee. Ensure usage complies with local laws and server access policies.
