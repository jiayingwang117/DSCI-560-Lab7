import mysql.connector
import re
from bs4 import BeautifulSoup
from datetime import datetime, date
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

def create_tables(cursor):
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS well_scraped_data_cleaned (
            id INT AUTO_INCREMENT PRIMARY KEY,
            well_name VARCHAR(255),
            api_number VARCHAR(50),
            well_status VARCHAR(255),
            well_type VARCHAR(255),
            closest_city VARCHAR(255),
            block_stats TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS well_info_cleaned (
            id INT AUTO_INCREMENT PRIMARY KEY,
            operator VARCHAR(255),
            api_number VARCHAR(50),
            well_name VARCHAR(255),
            enseco_job_number VARCHAR(50),
            job_type VARCHAR(255),
            county_state VARCHAR(255),
            well_shl VARCHAR(255),
            latitude DECIMAL(10, 6),
            longitude DECIMAL(10, 6),
            datum VARCHAR(50)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stimulation_data_cleaned (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date_stimulated DATE,
            stimulated_formation VARCHAR(255),
            top_depth FLOAT,
            bottom_depth FLOAT,
            stimulation_stages INT,
            volume FLOAT,
            volume_units VARCHAR(50),
            acid_percent FLOAT,
            lbs_proppant FLOAT,
            max_treatment_pressure FLOAT,
            max_treatment_rate FLOAT,
            proppant_details TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS proppant_details_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stimulation_data_id INT,
            proppant_type VARCHAR(255),
            proppant_value INT,
            FOREIGN KEY (stimulation_data_id) REFERENCES stimulation_data_cleaned(id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS block_stats_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            well_id INT,  
            quantity FLOAT,
            unit VARCHAR(255),
            month VARCHAR(50),
            year INT,
            FOREIGN KEY (well_id) REFERENCES well_scraped_data_cleaned(id)
        )
    """)

def clean_text(text, preserve_tags=False):
    if isinstance(text, datetime):
        text = text.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(text, date):
        text = text.strftime("%Y-%m-%d")
    if not text:
        return "N/A"
    
    # for "NA"
    cleaned_text = re.sub(r"\s+", "", str(text)).upper()

    if cleaned_text == "NA":
        return "N/A"
    
    if preserve_tags:
        text = BeautifulSoup(str(text), "html.parser").prettify()
    else:
        text = BeautifulSoup(str(text), "html.parser").get_text()
    
    text = re.sub(r"[^a-zA-Z0-9.,\s-]", "", text)  
    # remove : for operator
    text = re.sub(r":\s*", "", text) 
    text = re.sub(r"\s+", " ", text).strip() 
    
    return text if text else "N/A"

def convert_to_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0
    
def split_proppant_details(proppant_details):
    if proppant_details is None:
        return []  

    details_list = proppant_details.split('\n')
    proppant_data = []
    
    for detail in details_list:
        match = re.match(r"(.+): (\d+)", detail.strip())
        if match:
            proppant_data.append({
                'type': match.group(1).strip(),
                'value': int(match.group(2))
            })
    
    return proppant_data

def process_block_stats(block_stats, well_id, cursor):
    entries = block_stats.split(',')
    
    for entry in entries:
        entry = entry.strip()  
        
        match = re.search(r"([\d\.]+)\s*([kKmMbB]?)\s*(MCF|Barrels)\s+of\s+[a-zA-Z\s]+\s+in\s+([a-zA-Z]+)\s+(\d{4})", entry)
        
        if match:
            number = float(match.group(1))
            suffix = match.group(2).upper()
            unit = match.group(3).upper()
            month = match.group(4)
            year = int(match.group(5))
            
            if suffix == "K":
                quantity = number * 1000
            elif suffix == "M":
                quantity = number * 1000000
            elif suffix == "B":
                quantity = number * 1000000000
            else:
                quantity = number
            
            cursor.execute(
                """
                INSERT INTO block_stats_data (well_id, quantity, unit, month, year)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (well_id, quantity, unit, month, year)
            )
        else:
            logging.warning(f"Failed to match entry: '{entry}'")

def preprocess_data():
    conn = mysql.connector.connect(
        host=DB_HOST, 
        user=DB_USER, 
        password=DB_PASS, 
        database=DB_NAME
    )
    cursor = conn.cursor()
    
        # Disable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    
    create_tables(cursor)
    
    # Truncate the tables
    cursor.execute("TRUNCATE TABLE well_info_cleaned")
    cursor.execute("TRUNCATE TABLE stimulation_data_cleaned")
    cursor.execute("TRUNCATE TABLE proppant_details_data")
    cursor.execute("TRUNCATE TABLE well_scraped_data_cleaned")
    cursor.execute("TRUNCATE TABLE block_stats_data")
    
    # Re-enable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    
    cursor.execute("SELECT * FROM well_info")
    for well in cursor.fetchall():
        cursor.execute(
            """
            INSERT INTO well_info_cleaned (operator, api_number, well_name, enseco_job_number, job_type, county_state, well_shl, latitude, longitude, datum)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (clean_text(well[1]), clean_text(well[2]), clean_text(well[3]), clean_text(well[4]), 
             clean_text(well[5]), clean_text(well[6]), clean_text(well[7]), convert_to_float(well[8]), 
             convert_to_float(well[9]), clean_text(well[10]))
        )

    cursor.execute("SELECT * FROM stimulation_data")
    for stim in cursor.fetchall():
        cursor.execute(
            """
            INSERT INTO stimulation_data_cleaned (date_stimulated, stimulated_formation, top_depth, bottom_depth, stimulation_stages, volume, volume_units, acid_percent, lbs_proppant, max_treatment_pressure, max_treatment_rate, proppant_details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (stim[2], clean_text(stim[3]), convert_to_float(stim[4]), 
             convert_to_float(stim[5]), stim[6], convert_to_float(stim[7]), stim[8], convert_to_float(stim[9]), 
             convert_to_float(stim[10]), convert_to_float(stim[11]), convert_to_float(stim[12]), stim[13])
        )
        
        # Split proppant_details and insert into the new table
        proppant_data = split_proppant_details(stim[13])
        stimulation_data_id = cursor.lastrowid  # Get the id of the last inserted record
        
        for prop in proppant_data:
            cursor.execute(
                """
                INSERT INTO proppant_details_data (stimulation_data_id, proppant_type, proppant_value)
                VALUES (%s, %s, %s)
                """,
                (stimulation_data_id, prop['type'], prop['value'])
            )
    
    cursor.execute("SELECT * FROM well_scraped_data")
    for data in cursor.fetchall():
        well_name = clean_text(data[1])
        api_number = data[2]
        well_status = clean_text(data[3])
        well_type = clean_text(data[4])
        closest_city = clean_text(data[5])
        block_stats = clean_text(data[6], preserve_tags=True)
        
        cursor.execute(
            """
            INSERT INTO well_scraped_data_cleaned (well_name, api_number, well_status, well_type, closest_city, block_stats)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (well_name, api_number, well_status, well_type, closest_city, block_stats)
        )
        
        well_id = cursor.lastrowid  
        
        if block_stats != "N/A":
            process_block_stats(block_stats, well_id, cursor)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Data preprocessing completed and stored in cleaned tables.")

if __name__ == "__main__":
    preprocess_data()
