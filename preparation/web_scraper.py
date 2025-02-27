import mysql.connector
import requests
from bs4 import BeautifulSoup
import os
import re
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database credentials
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")


def get_well_data():
    """Fetch well_name and api_number from the MySQL database."""
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()

    cursor.execute("SELECT well_name, api_number FROM well_info")
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    # Debugging: Print retrieved wells
    for row in rows:
        print(f"Retrieved: {row}")

    return rows


def format_well_name(well_name):
    """Format well_name to match the website's URL structure."""
    well_name = well_name.lower().strip()  # Convert to lowercase and remove leading/trailing spaces
    well_name = re.sub(r"[^a-zA-Z0-9\s-]", "", well_name)  # Remove special characters
    well_name = well_name.replace(" ", "-")  # Replace spaces with hyphens
    return well_name


def fetch_well_data(well_name, api_number):
    """Scrape well data from the website."""
    formatted_well_name = format_well_name(well_name)  # Format well name for URL
    url = f"https://www.drillingedge.com/north-dakota/mckenzie-county/wells/{formatted_well_name}/{api_number}"
    
    print(f"Fetching data from: {url}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        data = {}

        # ✅ Locate the table with class "skinny" inside "table_wrapper"
        table_wrapper = soup.find("div", class_="table_wrapper")
        if table_wrapper:
            table = table_wrapper.find("table", class_="skinny")
            if table:
                rows = table.find_all("tr")  # Find all rows in the table
                
                for row in rows:
                    headers = row.find_all("th")  # Find headers
                    values = row.find_all("td")  # Find values
                    
                    for i in range(len(headers)):
                        key = headers[i].text.strip()
                        if i < len(values):  # Ensure corresponding <td> exists
                            value = values[i].text.strip()
                            if key in ["Well Status", "Well Type", "Closest City"]:
                                data[key] = value

        # ✅ Extract all data in <p class='block_stat'>
        block_stat_data = [stat.text.strip() for stat in soup.find_all("p", class_="block_stat")]
        data["Block Stats"] = block_stat_data

        print(f"✅ Scraped data for {well_name}: {data}")
        return data
    else:
        print(f"⚠️ ERROR: Failed to fetch data for {well_name}. Status Code: {response.status_code}")
        return {"error": f"Failed to fetch data. Status Code: {response.status_code}"}


def store_scraped_data(well_name, api_number, data):
    """Store scraped data into MySQL database."""
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS well_scraped_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            well_name VARCHAR(255),
            api_number VARCHAR(50),
            well_status VARCHAR(255),
            well_type VARCHAR(255),
            closest_city VARCHAR(255),
            block_stats TEXT
        )
    """)
    conn.commit()

    # Insert data into MySQL
    cursor.execute("""
        INSERT INTO well_scraped_data (well_name, api_number, well_status, well_type, closest_city, block_stats)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        well_name,
        api_number,
        data.get("Well Status", "N/A"),
        data.get("Well Type", "N/A"),
        data.get("Closest City", "N/A"),
        ", ".join(data.get("Block Stats", []))
    ))
    
    conn.commit()
    cursor.close()
    conn.close()


def main():
    """Main function to process all wells."""
    wells = get_well_data()
    for well_name, api_number in wells:
        if not well_name or not api_number:  # Skip invalid values
            print(f"⚠️ Skipping invalid well: {well_name} ({api_number})")
            continue

        scraped_data = fetch_well_data(well_name, api_number)
        store_scraped_data(well_name, api_number, scraped_data)
        print(f"✅ Data stored for {well_name} ({api_number})")


if __name__ == "__main__":
    main()