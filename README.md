Project Description: IG-Finder – Automated Instagram Profile Discovery and Matching System.

IG-Finder is a Python-based automation system designed to discover and match Instagram profiles from Google search results based on user-defined inputs in a Google Sheet. It continuously monitors the sheet, extracts relevant data, and searches for possible Instagram profiles using Google’s SERP (Search Engine Results Pages). The project integrates various components for scraping, profile matching, and Google Sheets automation.

⚙️ Key Features:
Google Sheets Integration: Monitors and updates sheets in real-time to read search queries and write results.

Automated Profile Extraction: Uses search queries to find Instagram profiles via web scraping.

Name & Identity Matching: Compares found Instagram usernames and names with provided data for accurate identification.

Continuous Monitoring: Keeps running to handle live updates to the sheet.

Credential Management: Reads secure API credentials from a credentials.json file.


Key Modules:
main.py: Main entry point that coordinates scraping, matching, and sheet updates.

InstagramDataExtractor.py: Performs Google searches to extract Instagram profile links.

ProfileMatcher.py: Logic to match names from input data with extracted Instagram profile metadata.

sheets_handler.py: Handles reading from and writing to Google Sheets.

sheets_monitor.py: Watches the Google Sheet for changes and triggers updates.


IG-Finder-main/
│
├── InstagramDataExtractor.py   # Core logic to extract Instagram profile data
├── ProfileMatcher.py           # Logic to match and score profiles based on metadata
├── main.py                     # Entrypoint to run the workflow
├── sheets_handler.py           # Google Sheets API utilities (read/write)
├── sheets_monitor.py           # Logic to watch input sheet and trigger processes
├── credentials.json            # Google Sheets API credentials (not to be shared)
├── requirements.txt            # List of dependencies
└── .gitignore                  # Git ignore rules





