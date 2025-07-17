# cops.py

**cops.py** is a Python script that monitors and stores active emergency calls from the Caddo 911 website. It parses data from a public webpage and logs structured call information into a MySQL database for long-term tracking, auditing, and analysis.

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

---

## 🗃️ Database Schema

Each agency gets its own table named `agency_<AGC>`, where `<AGC>` is the first 3 characters of the sanitized agency name.

Table schema includes:

- `Agency`, `Time`, `Units`, `Description`, `Street`, `CrossStreets`, `Municipal`
- `Hash`: Unique fingerprint (excluding units)
- `FirstSeen`, `LastSeen`: UTC timestamps
- `Resolved`: Marks calls that have disappeared from the source site
- Full-text indexes on `Description`, `Street`, and `CrossStreets`

---

## 🔁 How It Works

1. Every **35 seconds**, the script fetches the active calls page.
2. Parses the HTML table of calls.
3. For each call:
   - A hash (excluding `Units`) is computed.
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
pip install requests beautifulsoup4 mysql-connector-python
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
