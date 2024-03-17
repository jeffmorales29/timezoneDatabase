# Project : Timezone Database
# Author : Jeffrey Morales
#Email: jeffmorales29@gmail.com


import sqlite3
import requests

# Connect to SQLite database
conn = sqlite3.connect('TimezoneDB.db')
cursor = conn.cursor()

#API data
url = "http://api.timezonedb.com/v2.1/list-time-zone"
api_key = "VFUCYVC71XME"

# Create a table if not exists
cursor.execute('''
    CREATE TABLE IF NOT EXISTS TZDB_TIMEZONES (
        COUNTRYCODE VARCHAR2 NOT NULL,
        COUNTRYNAME VARCHAR2 NOT NULL,
        ZONENAME VARCHAR2 PRIMARY KEY NOT NULL,
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
        ZONESTART INTEGER NOT NULL,
        ZONEEND INTEGER NOT NULL,
        IMPORT_DATE DATE
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS TZDB_ZONE_DETAILS (
        COUNTRYCODE VARCHAR2 NOT NULL,
        COUNTRYNAME VARCHAR2 NOT NULL,
        ZONENAME VARCHAR2 NOT NULL,
        GMTOFFSET INTEGER NOT NULL,
        DST INTEGER NOT NULL,
        ZONESTART INTEGER NOT NULL,
        ZONEEND INTEGER NOT NULL,
        IMPORT_DATE DATE
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS TZDB_ERROR_LOG (
        ERROR_DATE DATE NOT NULL,
        ERROR_MESSAGE VARCHAR2 NOT NULL
    )
''')

#DELETES THE DATA BEFORE POPULATING A NEW REQUEST
cursor.execute('''
    DELETE FROM TZDB_TIMEZONES
''')


#PROGRAM START

#Set Parameter
params = {
    "key": api_key,
    "format": "json",  # Response format
}

# Make GET request
response = requests.get(url, params=params)
time_zones = response.json()["zones"]

# Check response status code
if response.status_code == 200:
    #populate our database
    for zone in time_zones:
        cursor.execute('''
        INSERT INTO TZDB_TIMEZONES (countryCode, countryName, zonename, gmtOffset, import_date)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
    ''', (zone["countryCode"], zone["countryName"], zone["zoneName"], zone["gmtOffset"]))
        
    print("Data from Timezone API Successfully loaded!\n")
    print("Please select an option:\n")
    print("""1. Load TZDB_Timezone
2. Load Timezone Details
3. Show Error log. """)

    def handle_option(option):
        switch = {
            '1': load_timezone,
            '2': load_timezone_details,
            '3': show_error_log,
        }
        # Get the function corresponding to the option and call it
        func = switch.get(option, invalid_option)
        func()

    def load_timezone():
        print("Loading TZDB_Timezone...")
        cursor.execute('''
        SELECT * FROM TZDB_TIMEZONES
    ''')
        rows = cursor.fetchall()

        print("COUNTRYCODE\tCOUNTRYNAME\tZONENAME\tGMTOFFSET\tIMPORT_DATE")

        for row in rows:
            country_code, country_name, zone_name, gmt_offset, import_date = row
            print(f"{country_code}\t\t{country_name}\t\t{zone_name}\t\t{gmt_offset}\t\t{import_date}")

    def load_timezone_details():
        print("Loading Timezone Details...")
        # Add code to load Timezone Details here

    def show_error_log():
        print("Showing Error log...")
        # Add code to show Error log here

    def invalid_option():
        print("Invalid option")

    # Main program
    print("Data from Timezone API Successfully loaded!\n")
    print("Please select an option:\n")
    print("""1. Load TZDB_Timezone
2. Load Timezone Details
3. Show Error log. """)
    option = input("Enter your choice: ")

    handle_option(option)

else:
    # Print error message
    print("Error:", response.status_code)