import requests
import xml.etree.ElementTree as ET
import mysql.connector
from mysql.connector import Error
import time
from datetime import datetime
import pytz

# MTConnect Agent URLs
urls = {
    "machine_name1 : http//ip address:portnumber/",
    "machine_name2 : http//ip address:portnumber/",
    "machine_name3 : http//ip address:portnumber/",
    "machine_name4 : http//ip address:portnumber/"
}

# MySQL database connection details
DB_CONFIG = {
    'host': 'hostname',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'your_database_name',
    'auth_plugin': 'mysql_native_password'
}

def create_table():
    conn = None  # Initialize conn to None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)  # Unpack the DB_CONFIG dictionary
        if conn.is_connected():
            cursor = conn.cursor()

            cursor.execute('''CREATE TABLE IF NOT EXISTS machine_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                machine_name VARCHAR(50),  -- Changed from INT to VARCHAR
                spindle_speed FLOAT, 
                emergency_stop VARCHAR(50), 
                last_cycle INT, 
                this_cycle INT, 
                run_status VARCHAR(50), 
                cycle_remaining_time INT, 
                last_cycle_timestamp DATETIME, 
                this_cycle_timestamp DATETIME, 
                added_timestamp DATETIME
            )''')
            print("Table 'machine_data' created/checked successfully.")

    except Error as e:
        print(f"Database error: {e}")

    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()

def insert_machine_data(machine_name, spindle_speed, emergency_stop,
                        last_cycle, this_cycle, run_status, cycle_remaining_time,
                        last_cycle_timestamp, this_cycle_timestamp, added_timestamp):
    conn = None  # Initialize conn to None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)  # Unpack the DB_CONFIG dictionary
        if conn.is_connected():
            cursor = conn.cursor()

            cursor.execute('''INSERT INTO machine_data (
                machine_name, spindle_speed, emergency_stop, 
                last_cycle, this_cycle, run_status, cycle_remaining_time, 
                last_cycle_timestamp, this_cycle_timestamp, added_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                (machine_name, spindle_speed, emergency_stop, last_cycle,
                 this_cycle, run_status, cycle_remaining_time,
                 last_cycle_timestamp, this_cycle_timestamp, added_timestamp))

            conn.commit()
            print(f"Data inserted successfully for Machine Name: {machine_name}.")

    except Error as e:
        print(f"Database error: {e}")

    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()

def convert_timestamp_to_mysql_format(iso_timestamp):
    try:
        parsed_timestamp = datetime.strptime(iso_timestamp, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
        ist_timestamp = parsed_timestamp.astimezone(pytz.timezone('Asia/Kolkata'))
        mysql_timestamp = ist_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        return mysql_timestamp
    except ValueError as e:
        print(f"Error converting timestamp: {e}")
        return None

def fetch_machine_data(machine_name, url):
    try:
        response = requests.get(url + "current")
        response.raise_for_status()

        root = ET.fromstring(response.content)
        namespaces = {'ns': 'urn:mtconnect.org:MTConnectStreams:1.2'}

        spindle_speed = root.find('.//ns:Samples/ns:SpindleSpeed', namespaces)
        device_stream = root.find('.//ns:DeviceStream', namespaces)
        emergency_stop = root.find('.//ns:Events/ns:EmergencyStop', namespaces)
        last_cycle = root.find('.//ns:Samples/ns:AccumulatedTime[@name="LastCycle"]', namespaces)
        this_cycle = root.find('.//ns:Samples/ns:AccumulatedTime[@name="ThisCycle"]', namespaces)
        run_status = root.find('.//ns:Events/ns:Execution', namespaces)
        cycle_remaining_time = root.find('.//ns:Samples/ns:AccumulatedTime[@name="CycleRemainingTime"]', namespaces)

        last_cycle_timestamp_iso = last_cycle.get('timestamp') if last_cycle is not None else None
        this_cycle_timestamp_iso = this_cycle.get('timestamp') if this_cycle is not None else None

        last_cycle_timestamp = convert_timestamp_to_mysql_format(last_cycle_timestamp_iso)
        this_cycle_timestamp = convert_timestamp_to_mysql_format(this_cycle_timestamp_iso)

        added_timestamp = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')

        spindle_speed_value = float(spindle_speed.text) if spindle_speed is not None else None
        emergency_stop_value = emergency_stop.text if emergency_stop is not None else None
        last_cycle_value = int(last_cycle.text) if last_cycle is not None else None
        this_cycle_value = int(this_cycle.text) if this_cycle is not None else None
        run_status_value = run_status.text if run_status is not None else None
        cycle_remaining_time_value = int(cycle_remaining_time.text) if cycle_remaining_time is not None else None

        # Insert the data into the database for the specific machine name
        insert_machine_data(machine_name, spindle_speed_value, emergency_stop_value,
                            last_cycle_value, this_cycle_value, run_status_value, cycle_remaining_time_value,
                            last_cycle_timestamp, this_cycle_timestamp, added_timestamp)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from the machine {machine_name}: {e}")

# Create the table before fetching data
create_table()

# Run the function for multiple machines
while True:
    for machine_name, url in urls.items():
        fetch_machine_data(machine_name, url)
    time.sleep(5)  # Wait for 5 seconds before the next fetch
