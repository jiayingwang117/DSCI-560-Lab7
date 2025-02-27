// server.js
const express = require('express');
const mysql = require('mysql2');
const cors = require('cors'); // Import cors module

const app = express();
const port = 3000;

// Enable CORS for all requests
app.use(cors());

// MySQL connection settings
const connection = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'Ashley020501',
  database: 'oil_well_db'
});

// Connect to MySQL
connection.connect(err => {
  if (err) {
    console.error('Error connecting: ' + err.stack);
    return;
  }
  console.log('Connected to MySQL as id ' + connection.threadId);
});

// API endpoint to retrieve well info (including latitude and longitude)
app.get('/api/wells', (req, res) => {
  res.header("Access-Control-Allow-Origin", "*"); // Explicitly allow CORS
  res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");

  const query = `
    SELECT wi.id, wi.operator, wi.api_number, wi.well_name, wi.county_state, wi.well_shl,
           wi.latitude, wi.longitude, wsd.block_stats, sd.date_stimulated, sd.stimulated_formation
    FROM well_info wi
    LEFT JOIN well_scraped_data wsd ON wi.api_number = wsd.api_number
    LEFT JOIN stimulation_data sd ON wi.id = sd.well_info_id
    ORDER BY wi.id DESC
  `;

  connection.query(query, (error, results) => {
    if (error) throw error;
    res.json(results);
  });
});

// Start the server
app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
