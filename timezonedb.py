# Project : Timezone Database
# Author : Jeffrey Morales
# Email: jeffmorales29@gmail.com

import sqlite3
import requests
from datetime import datetime

# Connect to SQLite database
conn = sqlite3.connect('TimezoneDB.db')
cursor = conn.cursor()


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
    CREATE TABLE IF NOT EXISTS TZDB_ERROR_LOG (
        ERROR_DATE DATE NOT NULL,
        ERROR_MESSAGE VARCHAR2 NOT NULL
    )
''')

#TEMPORARY TIMEZONE DETAILS TABLE
cursor.execute('''
    CREATE TABLE IF NOT EXISTS TZDB_DETAILS_STAGINGTB (
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

#DELETES THE DATA BEFORE POPULATING A NEW REQUEST
cursor.execute('''
    DELETE FROM TZDB_TIMEZONES
''')

#API Key
api_key = "VFUCYVC71XME"

#Loading Message
print("Getting data from API...")

#API request
def time_zone(key):
    parameters = {
        'key': key,
        'format': 'json'
    }
    response = requests.get('http://api.timezonedb.com/v2.1/list-time-zone', params=parameters)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("STATUS CODE NOT 200. STATUS CODE: " + str(response.status_code))
    
def get_details(key, zones):
    parameters = {
        'key': key,
        'format': 'json',
        'by': 'zone',
        'zone': zones
    }
    response = requests.get('http://api.timezonedb.com/v2.1/get-time-zone', params=parameters)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("STATUS CODE NOT 200. STATUS CODE: " + str(response.status_code))
    


#Save Error log to Database
def insert_error(message,c,conn):
    time_now = datetime.now().strftime('%m-%d-%Y %H:%M:%S')
    c.execute(f''' 
            INSERT INTO TZDB_ERROR_LOG 
            (ERROR_DATE, ERROR_MESSAGE)
            VALUES
            ( '{time_now}','{message}');
            ''')
    conn.commit()

   
time_zone_response = time_zone(api_key)

#Insert API Data into timezone database
try:
    for response in time_zone_response['zones']:
        time_now = datetime.now().strftime('%m-%d-%Y %H:%M:%S')
        cursor.execute(f'''
        INSERT INTO TZDB_TIMEZONES
        (COUNTRYCODE, COUNTRYNAME, ZONENAME, GMTOFFSET, IMPORT_DATE)
        VALUES
        ('{response['countryCode']}', '{response['countryName']}', '{response['zoneName']}', 
        {response['gmtOffset']}, '{time_now}');
        ''')
except Exception as e:
        insert_error('ERROR INSERTING INTO TZDB_TIMEZONES TABLE, ERROR :' +str(e), cursor,conn)

##Insert API Data into timezone details database
try:
    for response in time_zone_response['zones']:
        try:
            time_now = datetime.now().strftime('%m-%d-%Y %H:%M:%S')
            details_response = get_details(api_key, zones=response['zoneName'])

            # Check if the record already exists in the main table
            cursor.execute(f'''
                SELECT ZONENAME FROM TZDB_ZONE_DETAILS WHERE ZONENAME = '{details_response['zoneName']}'
            ''')
            existing_record = cursor.fetchone()

            if not existing_record:
                # Insert into the staging table
                cursor.execute(f'''
                    INSERT INTO TZDB_DETAILS_STAGINGTB 
                    (ZONENAME,ZONESTART,ZONEEND,COUNTRYCODE,
                    COUNTRYNAME ,GMTOFFSET ,DST,IMPORT_DATE)
                    VALUES
                    ('{details_response['zoneName']}', '{details_response['zoneStart']}', '{details_response['zoneEnd']}',
                    '{details_response['countryCode']}', '{details_response['countryName']}', '{details_response['gmtOffset']}',
                    '{details_response['dst']}', '{time_now}');
                ''')
                conn.commit()

        except Exception as e:
            insert_error('ERROR INSERTING INTO STAGING TABLE, ERROR: ' + str(e), cursor, conn)

        # Insert records from staging table to main table, avoiding duplicates
        cursor.execute('''
        INSERT INTO TZDB_ZONE_DETAILS
        (COUNTRYCODE, COUNTRYNAME, ZONENAME, GMTOFFSET ,DST , ZONESTART, ZONEEND, IMPORT_DATE)
        SELECT * FROM TZDB_DETAILS_STAGINGTB 
        WHERE ZONENAME NOT IN (SELECT ZONENAME FROM TZDB_ZONE_DETAILS);
        ''')
        conn.commit()

        # Clean up the staging table
        cursor.execute('''DELETE FROM TZDB_DETAILS_STAGINGTB''')
        conn.commit()

except Exception as e:
    insert_error('ERROR PARSING TIMEZONE RESPONSE, ERROR :' + str(e), cursor,conn)


#Script Menu
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
    #Show the timezone database
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
    #Show the timezone details
    print("Loading Timezone Details...")
    cursor.execute('''
    SELECT * FROM TZDB_ZONE_DETAILS
''')
    rows = cursor.fetchall()
    print("COUNTRYCODE\tCOUNTRYNAME\tZONENAME\tGMTOFFSET\tDST\tZONESTART\tZONEEND\tIMPORT_DATE")

    for row in rows:
        country_code, country_name, zone_name, gmt_offset, dst, zonestart, zoneend ,import_date = row
        print(f"{country_code}\t\t{country_name}\t\t{zone_name}\t\t{gmt_offset}\t\t{dst}\t\t{zonestart}\t\t{zoneend}\t\t{import_date}")


def show_error_log():
    #Display Error Logs
    print("Showing Error log...")
    cursor.execute('''
    SELECT * FROM TZDB_ERROR_LOG
''')
    rows = cursor.fetchall()
    print("ERRORDATE\tERRORMESSAGE")

    for row in rows:
        error_date, error_message, = row
        print(f"{error_date}\t\t{error_message}")


def invalid_option():
    print("Invalid option")

option = input("\nEnter your choice: ")

handle_option(option)