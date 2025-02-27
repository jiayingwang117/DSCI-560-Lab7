# DSCI-560-Lab7
## Preparation
Before running the main setup, ensure that the database has been correctly prepared. This involves running the following scripts in the `preparation` folder:

1. `pdf_parser.py`
2. `web_scraper.py`
3. `data_preprocessing.py`

Make sure that these scripts execute successfully, as they populate the database with the necessary data.

---

## 1) Initial Setup
This project utilizes web-related tools, platforms, and a MySQL database. Please use a Linux environment for the setup and execution.

### Installation Steps
- Ensure `Node.js` and `npm` are installed:
  ```bash
  sudo apt update
  sudo apt install nodejs npm
  ```
- Install `MySQL Server`:
  ```bash
  sudo apt install mysql-server
  ```
---

## 2) Webpage and Mapping
The goal is to create an interactive map displaying well locations with additional data. This will be achieved by setting up an Apache web server and integrating map APIs.

### Web Server Setup
- Install Apache:
  ```bash
  sudo apt install apache2
  ```
- Start the Apache service:
  ```bash
  sudo systemctl start apache2
  ```
- Enable Apache to run on startup:
  ```bash
  sudo systemctl enable apache2
  ```

### Running the Server and Database Connection
1. **Run the Node.js backend:**
   ```bash
   node server.js
   ```
   Expected output:
   ```
   Server running at http://localhost:3000
   Connected to MySQL as id xxx
   ```

2. **Open a new terminal window and start a local HTTP server for the frontend:**
   ```bash
   python -m http.server 8000
   ```
   Now, you can:
   - View the well data in JSON format at: `http://localhost:3000/api/wells`
   - See the interactive map displaying the well locations at: `http://localhost:8000/index.html`

---
