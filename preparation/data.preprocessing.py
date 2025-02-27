import mysql.connector
import re
from bs4 import BeautifulSoup
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MySQL connection configuration
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

def clean_text(text, preserve_tags=False):
    """
    Clean text data by removing HTML tags, special characters, and extra spaces. 
    Optionally preserve specific HTML tags.
    """
    if not text:
        return "N/A"
    if preserve_tags:
        text = BeautifulSoup(text, "html.parser").prettify()  
    else: 
        text = BeautifulSoup(text, "html.parser").get_text()  
      
    # Keep letters, numbers, and basic symbols
    text = re.sub(r"[^a-zA-Z0-9.,:\s-]", "", text)  
    # Remove extra spaces
    text = re.sub(r"\s+", " ", text).strip()  
    
    return text if text else "N/A"

def convert_to_float(value):
    """
    Convert numerical string to float, default to 0 if missing.
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def convert_to_date(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str  
    except (ValueError, TypeError):
        return date_str 

def preprocess_data():
    """
    Read data from MySQL, clean and transform it, then update the database.
    """
    conn = mysql.connector.connect(
        host=DB_HOST, 
        user=DB_USER, 
        password=DB_PASS, 
        database=DB_NAME
    )
    cursor = conn.cursor(dictionary=True)
    
    # Process well_info table, selecting only necessary fields
    cursor.execute("SELECT id, well_name, latitude, longitude FROM well_info")
    wells = cursor.fetchall()
    for well in wells:
        cleaned_data = {key: clean_text(value) for key, value in well.items() if isinstance(value, str)}
        cursor.execute(
            """
            UPDATE well_info 
            SET well_name = %s, latitude = %s, longitude = %s
            WHERE id = %s
            """, 
            (cleaned_data.get('well_name'), cleaned_data.get('latitude'), cleaned_data.get('longitude'), well["id"])
        )


    # Process stimulation_data table
    cursor.execute("SELECT id, date_stimulated, top_depth, bottom_depth, volume, acid_percent FROM stimulation_data")
    stimulations = cursor.fetchall()
    for stim in stimulations:
        cleaned_data = {
            "date_stimulated": convert_to_date(stim["date_stimulated"]),
            "top_depth": convert_to_float(stim["top_depth"]),
            "bottom_depth": convert_to_float(stim["bottom_depth"]),
            "volume": convert_to_float(stim["volume"]),
            "acid_percent": convert_to_float(stim["acid_percent"]),
        }
        cursor.execute(
            """
            UPDATE stimulation_data 
            SET date_stimulated = %s, top_depth = %s, bottom_depth = %s, volume = %s, acid_percent = %s
            WHERE id = %s
            """, 
        (
            cleaned_data['date_stimulated'] if cleaned_data['date_stimulated'] is not None else None,
            cleaned_data['top_depth'], cleaned_data['bottom_depth'], 
            cleaned_data['volume'], cleaned_data['acid_percent'], stim["id"])
        )
    
    # Process well_scraped_data table
    cursor.execute("SELECT id, well_status, well_type, closest_city, block_stats FROM well_scraped_data")
    scraped_data = cursor.fetchall()
    for data in scraped_data:
        cleaned_data = {key: clean_text(value) for key, value in data.items() if isinstance(value, str)}
        cursor.execute(
            """
            UPDATE well_scraped_data 
            SET well_status = %s, well_type = %s, closest_city = %s, block_stats = %s
            WHERE id = %s
            """, 
            (cleaned_data.get('well_status'), cleaned_data.get('well_type'), 
            cleaned_data.get('closest_city'), cleaned_data.get('block_stats'), data["id"])
        )
    
    if cursor.rowcount > 0:
        conn.commit()
    
    cursor.close()
    conn.close()
    print("Data preprocessing completed.")

if __name__ == "__main__":
    preprocess_data()
