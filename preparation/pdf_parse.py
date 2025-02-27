#!/usr/bin/env python3

import os
import re
import subprocess
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime

import pdfplumber
import pytesseract
from pdf2image import convert_from_path

# Load environment variables from .env file
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# ---------------------------------------------------------------------------
# 1. Database connection and table creation
# ---------------------------------------------------------------------------

def create_db_and_tables():
    """
    Create a MySQL database (if not exists) and tables needed for storing
    the oil well information and stimulation data.
    """
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    conn.commit()
    cursor.close()
    conn.close()

    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )
    cursor = conn.cursor()

    create_well_info_table = """
    CREATE TABLE IF NOT EXISTS well_info (
        id INT AUTO_INCREMENT PRIMARY KEY,
        operator VARCHAR(255),
        api_number VARCHAR(50),
        well_name VARCHAR(255),
        enseco_job_number VARCHAR(50),
        job_type VARCHAR(255),
        county_state VARCHAR(255),
        well_shl VARCHAR(255),
        latitude VARCHAR(50),
        longitude VARCHAR(50),
        datum VARCHAR(50)
    );
    """

    cursor.execute(create_well_info_table)

    create_stimulation_table = """
    CREATE TABLE IF NOT EXISTS stimulation_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        well_info_id INT,
        date_stimulated DATE,
        stimulated_formation VARCHAR(255),
        top_depth VARCHAR(50),
        bottom_depth VARCHAR(50),
        stimulation_stages INT,
        volume VARCHAR(50),
        volume_units VARCHAR(50),
        acid_percent VARCHAR(50),
        lbs_proppant VARCHAR(50),
        max_treatment_pressure VARCHAR(50),
        max_treatment_rate VARCHAR(50),
        proppant_details TEXT,
        FOREIGN KEY (well_info_id) REFERENCES well_info(id)
    )
    """

    cursor.execute(create_stimulation_table)
    conn.commit()
    cursor.close()
    conn.close()

# ---------------------------------------------------------------------------
# 2. OCR and PDF text extraction
# ---------------------------------------------------------------------------

def ocr_pdf_to_text(pdf_path, temp_ocr_pdf_path="temp_ocr_output.pdf"):
    """
    Use ocrmypdf to create a text-searchable PDF from a scanned PDF,
    then return extracted text via pdfplumber.
    """
    # Run ocrmypdf to produce an OCR'd PDF
    subprocess.run(["ocrmypdf", "--force-ocr", pdf_path, temp_ocr_pdf_path], check=True)

    # Extract text from the newly created PDF
    text_content = ""
    with pdfplumber.open(temp_ocr_pdf_path) as pdf:
        for page in pdf.pages:
            text_content += (page.extract_text() or "") + "\n"

    # Remove temporary OCR PDF
    os.remove(temp_ocr_pdf_path)
    return text_content

def extract_text_from_pdf(pdf_path, min_chars_per_page=30, dpi=300):
    """
    Extract text from a PDF by first using pdfplumber, and if a page has 
    insufficient text, run OCR on that page using pytesseract.
    """
    full_text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            for page_number, page in enumerate(pdf.pages, start=1):
                extracted_text = page.extract_text() or ""
                if len(extracted_text.strip()) < min_chars_per_page:
                    print(f"[INFO] Page {page_number}/{total_pages} has insufficient text, running OCR...")
                    images = convert_from_path(pdf_path, first_page=page_number, last_page=page_number, dpi=dpi)
                    if images:
                        ocr_text = pytesseract.image_to_string(images[0])
                        combined_text = extracted_text.strip() + "\n" + ocr_text.strip()
                        extracted_text = combined_text.strip()
                else:
                    print(f"[INFO] Page {page_number}/{total_pages} extracted via pdfplumber.")
                full_text += extracted_text + "\n"
    except Exception as e:
        print(f"[ERROR] Failed to process PDF: {e}")
    return full_text

# ---------------------------------------------------------------------------
# 3. Parsing logic
# ---------------------------------------------------------------------------

def parse_well_info(text):
    """
    Parse the well info fields from the text.
    Returns a dictionary with the well info.
    """
    section_match = re.search(r"(?:Well Information|WELL DATA SUMMARY|SYNOPSIS)(.*)", text, re.DOTALL | re.IGNORECASE)
    section_text = section_match.group(1) if section_match else text

    operator_pattern       = r"Operator\s*(.+)"
    api_pattern            = r"API\s*(?:#|#:|NUMBER)\s*[:\-]?\s*([0-9\-]+)"
    well_name_pattern      = r"Well Name:\s*([^\n]+)"
    enseco_job_pattern     = r"Enseco\s*Job\s*#:\s*([^\s]+)"
    job_type_pattern       = r"Well\s*Type:\s*([^\n]+)"
    county_state_pattern   = r"County[/,]\s*State\s*(.+)"
    shl_pattern            = r"Surface Location\s*(.+)"
    datum_pattern          = r"Datum:\s*([^\n]+)"

    # Updated DMS patterns for latitude and longitude:
    latitude_pattern       = r'(?i)LATITUDE\s*:?\s*(\d{1,3})\s*(?:°|[^\d\w])\s*(\d{1,2})\s*(?:\'|’)?\s*(\d{1,2}(?:[.,]\d+)?)(?:\"|°|”)?\s*([NS])'
    longitude_pattern      = r'(?i)LONGITUDE\s*:?\s*(\d{1,3})\s*(?:°|[^\d\w])\s*(\d{1,2})\s*(?:\'|’)?\s*(\d{1,2}(?:[.,]\d+)?)(?:\"|°|”)?\s*([EW])'

    well_data = {
        "operator": None,
        "api_number": None,
        "well_name": None,
        "enseco_job_number": None,
        "job_type": None,
        "county_state": None,
        "well_shl": None,
        "latitude": None,
        "longitude": None,
        "datum": None
    }

    def match_and_set(pattern, key):
        m = re.search(pattern, section_text, re.IGNORECASE)
        if m:
            well_data[key] = m.group(1).strip()

    match_and_set(operator_pattern, "operator")
    match_and_set(api_pattern, "api_number")
    match_and_set(well_name_pattern, "well_name")
    match_and_set(enseco_job_pattern, "enseco_job_number")
    match_and_set(job_type_pattern, "job_type")
    match_and_set(county_state_pattern, "county_state")
    match_and_set(shl_pattern, "well_shl")
    match_and_set(datum_pattern, "datum")

    def dms_to_decimal(deg, minutes, seconds, direction):
        # Replace comma with dot for proper float conversion.
        seconds = seconds.replace(',', '.')
        dec = float(deg) + float(minutes) / 60.0 + float(seconds) / 3600.0
        if direction.upper() in ['S', 'W']:
            dec = -dec
        return dec

    # Process latitude if present in DMS format.
    lat_match = re.search(latitude_pattern, section_text, re.IGNORECASE)
    if lat_match:
        deg, minutes, seconds, direction = lat_match.groups()
        well_data["latitude"] = dms_to_decimal(deg, minutes, seconds, direction)
    
    # Process longitude if present in DMS format.
    lon_match = re.search(longitude_pattern, section_text, re.IGNORECASE)
    if lon_match:
        deg, minutes, seconds, direction = lon_match.groups()
        well_data["longitude"] = dms_to_decimal(deg, minutes, seconds, direction)
    
    print(well_data["latitude"], well_data["longitude"])
    return well_data



# --- Stimulation Parsing Functions ---

# Function tuned for Document 1 (e.g. with longer formation names)
def parse_stimulation_data_doc1(text):
    import re
    from datetime import datetime
    lines = text.splitlines()
    data = {
        "date_stimulated": None,
        "stimulated_formation": None,
        "type_treatment": None,
        "top_depth": None,
        "bottom_depth": None,
        "stimulation_stages": None,
        "volume": None,
        "volume_units": None,
        "acid_percent": None,
        "lbs_proppant": None,
        "max_treatment_pressure": None,
        "max_treatment_rate": None,
        "proppant_details": None,
    }
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        if "Date Stimulated" in line_stripped:
            if i+1 < len(lines):
                next_line = lines[i+1].strip()
                # Use positive lookahead to capture full formation names (doc1)
                m = re.match(
                    r"(\d{2}/\d{2}/\d{4})\s+(.+?)(?=\s+\d{4,})\s+(\d+)\s+(\d+)\s+(\d+)\s*[|I]\s+(\d+)\s+(\w+)",
                    next_line
                )
                if m:
                    try:
                        raw_date = m.group(1).strip()
                        date_obj = datetime.strptime(raw_date, "%m/%d/%Y")
                        data["date_stimulated"] = date_obj.strftime("%Y-%m-%d")
                    except Exception:
                        data["date_stimulated"] = None
                    data["stimulated_formation"] = m.group(2).strip()
                    data["top_depth"] = m.group(3).strip()
                    data["bottom_depth"] = m.group(4).strip()
                    data["stimulation_stages"] = m.group(5).strip()
                    data["volume"] = m.group(6).strip()
                    data["volume_units"] = m.group(7).strip()
        if "Type Treatment" in line_stripped:
            if i+1 < len(lines):
                next_line = lines[i+1].strip()
                m = re.match(r"(.+?)\s+(\d+)\s+(\d+)\s+([\d.]+)", next_line)
                if m:
                    data["type_treatment"] = m.group(1).strip()
                    data["lbs_proppant"] = m.group(2).strip()
                    data["max_treatment_pressure"] = m.group(3).strip()
                    data["max_treatment_rate"] = m.group(4).strip()
        if "Details" in line_stripped:
            details_lines = []
            for j in range(i+1, len(lines)):
                l = lines[j].strip()
                if l == "" or l.startswith("Date Stimulated") or l.startswith("Type Treatment"):
                    break
                details_lines.append(l)
            if details_lines:
                data["proppant_details"] = "\n".join(details_lines)
            break
    return data

# Function tuned for Document 2 (more tolerant separator)
def parse_stimulation_data_doc2(text):
    import re
    from datetime import datetime
    lines = text.splitlines()
    data = {
        "date_stimulated": None,
        "stimulated_formation": None,
        "type_treatment": None,
        "top_depth": None,
        "bottom_depth": None,
        "stimulation_stages": None,
        "volume": None,
        "volume_units": None,
        "acid_percent": None,
        "lbs_proppant": None,
        "max_treatment_pressure": None,
        "max_treatment_rate": None,
        "proppant_details": None,
    }
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        if "Date Stimulated" in line_stripped:
            if i+1 < len(lines):
                next_line = lines[i+1].strip()
                # This regex accepts either an explicit separator or extra spaces (doc2)
                m = re.match(
                    r"(\d{2}/\d{2}/\d{4})\s+(.+?)\s+(\d+)\s+(\d+)(?:\s*[|I]\s*|\s+)(\d+)\s+(\d+)\s+(\w+)",
                    next_line
                )
                if m:
                    try:
                        raw_date = m.group(1).strip()
                        date_obj = datetime.strptime(raw_date, "%m/%d/%Y")
                        data["date_stimulated"] = date_obj.strftime("%Y-%m-%d")
                    except Exception:
                        data["date_stimulated"] = None
                    data["stimulated_formation"] = m.group(2).strip()
                    data["top_depth"] = m.group(3).strip()
                    data["bottom_depth"] = m.group(4).strip()
                    data["stimulation_stages"] = m.group(5).strip()
                    data["volume"] = m.group(6).strip()
                    data["volume_units"] = m.group(7).strip()
        if "Type Treatment" in line_stripped:
            if i+1 < len(lines):
                next_line = lines[i+1].strip()
                m = re.match(r"(.+?)\s+(\d+)\s+(\d+)\s+([\d.]+)", next_line)
                if m:
                    data["type_treatment"] = m.group(1).strip()
                    data["lbs_proppant"] = m.group(2).strip()
                    data["max_treatment_pressure"] = m.group(3).strip()
                    data["max_treatment_rate"] = m.group(4).strip()
        if "Details" in line_stripped:
            details_lines = []
            for j in range(i+1, len(lines)):
                l = lines[j].strip()
                if l == "" or l.startswith("Date Stimulated") or l.startswith("Type Treatment"):
                    break
                details_lines.append(l)
            if details_lines:
                data["proppant_details"] = "\n".join(details_lines)
            break
    return data

def merge_stimulation_data(data1, data2):
    """
    For each key in the set of required stimulation fields, if data1 is None
    and data2 has a non-null value, update data1.
    """
    keys = ["date_stimulated", "stimulated_formation", "top_depth", "bottom_depth",
            "stimulation_stages", "volume", "volume_units"]
    for key in keys:
        if data1.get(key) is None and data2.get(key) is not None:
            data1[key] = data2[key]
    return data1

# ---------------------------------------------------------------------------
# 4. Insert data into the database
# ---------------------------------------------------------------------------

def insert_well_info(well_data, host="localhost", user="root", password="root", database="oil_well_data"):
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = conn.cursor()
    insert_sql = """
    INSERT INTO well_info (
        operator,
        api_number,
        well_name,
        enseco_job_number,
        job_type,
        county_state,
        well_shl,
        latitude,
        longitude,
        datum
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        well_data["operator"],
        well_data["api_number"],
        well_data["well_name"],
        well_data["enseco_job_number"],
        well_data["job_type"],
        well_data["county_state"],
        well_data["well_shl"],
        well_data["latitude"],
        well_data["longitude"],
        well_data["datum"]
    )
    cursor.execute(insert_sql, values)
    conn.commit()
    well_info_id = cursor.lastrowid
    cursor.close()
    conn.close()
    return well_info_id

def insert_stimulation_data(stim_data, well_info_id, host="localhost", user="root", password="root", database="oil_well_data"):
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = conn.cursor()
    insert_sql = """
    INSERT INTO stimulation_data (
        well_info_id,
        date_stimulated,
        stimulated_formation,
        top_depth,
        bottom_depth,
        stimulation_stages,
        volume,
        volume_units,
        acid_percent,
        lbs_proppant,
        max_treatment_pressure,
        max_treatment_rate,
        proppant_details
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        well_info_id,
        stim_data["date_stimulated"],
        stim_data["stimulated_formation"],
        stim_data["top_depth"],
        stim_data["bottom_depth"],
        stim_data["stimulation_stages"],
        stim_data["volume"],
        stim_data["volume_units"],
        stim_data["acid_percent"],
        stim_data["lbs_proppant"],
        stim_data["max_treatment_pressure"],
        stim_data["max_treatment_rate"],
        stim_data["proppant_details"]
    )
    cursor.execute(insert_sql, values)
    conn.commit()
    cursor.close()
    conn.close()

# ---------------------------------------------------------------------------
# 5. Main script
# ---------------------------------------------------------------------------

def main():
    create_db_and_tables()
    pdf_folder = "pdf_folder"

    for filename in os.listdir(pdf_folder):
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(pdf_folder, filename)
            print(f"Processing PDF: {pdf_path}")

            # Extract text (using OCR if needed)
            text_content = extract_text_from_pdf(pdf_path)

            # Save extracted text to a file
            output_filename = os.path.basename(pdf_path).replace(".pdf", "_extracted.txt")
            output_path = os.path.join("extracted_texts", output_filename)
            os.makedirs("extracted_texts", exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(text_content)
            print(f"Extracted text saved to: {output_path}")

            # Parse well info
            well_info = parse_well_info(text_content)
            well_info_id = insert_well_info(well_info, host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
            print(f"Inserted well info with ID: {well_info_id}")

            # First, try the doc1 version of stimulation parsing
            stim_data = parse_stimulation_data_doc1(text_content)
            # If any required field is missing, run the doc2 version and merge
            required_keys = ["date_stimulated", "stimulated_formation", "top_depth", "bottom_depth", "stimulation_stages", "volume", "volume_units"]
            if any(stim_data.get(key) is None for key in required_keys):
                stim_data_doc2 = parse_stimulation_data_doc2(text_content)
                stim_data = merge_stimulation_data(stim_data, stim_data_doc2)
            # print(stim_data)

            if stim_data["date_stimulated"] or stim_data["stimulated_formation"]:
                insert_stimulation_data(stim_data, well_info_id, host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
                print("Inserted stimulation data.")

    print("Done processing all PDFs.")

if __name__ == "__main__":
    main()
