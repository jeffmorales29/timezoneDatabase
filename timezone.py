# Project : Timezone Database
# Author : Jeffrey Morales
#Email: jeffmorales29@gmail.com


import sqlite3
import requests

# Connect to SQLite database
conn = sqlite3.connect('TimezoneDB.db')
cursor = conn.cursor()

# Create a table if not exists
cursor.execute('''
    CREATE TABLE IF NOT EXISTS TZDB_TIMEZONES (
        COUNTRYCODE VARCHAR2 NOT NULL,
        COUNTRYNAME VARCHAR2 PRIMARY KEY NOT NULL,
        ZONENAME VARCHAR2 NOT NULL,
        GMTOFFSET INTEGER,
        IMPORT_DATE DATE
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS TZDB_ZONE_DETAILS (
        COUNTRYCODE VARCHAR2 NOT NULL,
        COUNTRYNAME VARCHAR2 PRIMARY KEY NOT NULL,
        ZONENAME VARCHAR2 NOT NULL,
        GMTOFFSET INTEGER NOT NULL,
        DST INTEGER NOT NULL,
        ZONESTART INTEGER PRIMARY KEY NOT NULL,
        ZONEEND INTEGER PRIMARY KEY NOT NULL,
        IMPORT_DATE DATE
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS TZDB_ZONE_DETAILS (
        COUNTRYCODE VARCHAR2 NOT NULL,
        COUNTRYNAME VARCHAR2 PRIMARY KEY NOT NULL,
        ZONENAME VARCHAR2 NOT NULL,
        GMTOFFSET INTEGER NOT NULL,
        DST INTEGER NOT NULL,
        ZONESTART INTEGER PRIMARY KEY NOT NULL,
        ZONEEND INTEGER PRIMARY KEY NOT NULL,
        IMPORT_DATE DATE
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS TZDB_ERROR_LOG (
        ERROR_DATE DATE NOT NULL,
        ERROR_MESSAGE VARCHAR2 NOT NULL
    )
''')


#API data
url = "http://api.timezonedb.com/v2.1/list-time-zone"
api_key = "VFUCYVC71XME"

location = "London"

params = {
    "key": api_key,
    "format": "json",  # Response format
    "by": "zone",      # Search by time zone
    "zone": location  # Location parameter
}

# Make GET request
response = requests.get(url, params=params)

# Check response status code
if response.status_code == 200:
    # Print response content
    print(response.json())
else:
    # Print error message
    print("Error:", response.status_code)