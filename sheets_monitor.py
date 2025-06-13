"""
Google Sheets Trigger Monitor for Instagram Scraper
- Monitors a specific cell in Google Sheets 
- Starts/stops main.py based on dropdown value
"""
import time
import subprocess
import os
import signal
import sys
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Configuration
SHEET_NAME = "IG Data"
TRIGGER_WORKSHEET = "Trigger"
TRIGGER_CELL = "A2"
STATUS_CELL = "B2"
CREDENTIALS_FILE = "credentials.json"
CHECK_INTERVAL = 15  # seconds between checks

# Global variables
scraper_process = None
current_trigger_value = None


def initialize_sheets_client():
    """Initialize Google Sheets client"""
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    return gspread.authorize(credentials)


def get_trigger_sheet(sheets_client):
    """Get the trigger worksheet"""
    try:
        sheet = sheets_client.open(SHEET_NAME)
        return sheet.worksheet(TRIGGER_WORKSHEET)
    except Exception as e:
        print(f"Error accessing trigger sheet: {e}")
        return None


def start_scraper():
    """Start the Instagram scraper process"""
    global scraper_process
    
    # Kill any existing process first
    if scraper_process is not None and scraper_process.poll() is None:
        stop_scraper()
    
    print("Starting Instagram scraper process...")
    
    # Start the main.py script as a subprocess
    scraper_process = subprocess.Popen(['python3', 'main.py'], 
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE,
                                     text=True)
    
    print(f"Scraper process started with PID: {scraper_process.pid}")
    return True


def stop_scraper():
    """Stop the Instagram scraper process"""
    global scraper_process
    
    if scraper_process is None or scraper_process.poll() is not None:
        print("No scraper process is running")
        return False
    
    print(f"Stopping scraper process (PID: {scraper_process.pid})...")
    
    # Kill the process
    try:
        scraper_process.terminate()
        
        # Wait for up to 5 seconds for the process to terminate
        try:
            scraper_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            # If it doesn't terminate gracefully, force kill it
            scraper_process.kill()
            
        print("Scraper process stopped successfully")
        scraper_process = None
        return True
    except Exception as e:
        print(f"Error stopping scraper process: {e}")
        return False


def monitor_sheet():
    """Monitor the Google Sheet for trigger changes"""
    global current_trigger_value
    global scraper_process
    
    print(f"Starting to monitor {SHEET_NAME} > {TRIGGER_WORKSHEET} > {TRIGGER_CELL}")
    
    sheets_client = initialize_sheets_client()
    trigger_sheet = get_trigger_sheet(sheets_client)
    
    if trigger_sheet is None:
        print("Failed to access trigger sheet, exiting")
        return
    
    # Update status to Ready
    try:
        trigger_sheet.update_acell(STATUS_CELL, "Ready")
        print("Sheet status updated to Ready")
    except Exception as e:
        print(f"Error updating status cell: {e}")
    
    # Main monitoring loop
    while True:
        try:
            # Get the current trigger value
            new_trigger_value = trigger_sheet.acell(TRIGGER_CELL).value
            
            # Check if status shows "Error" and fix it
            current_status = trigger_sheet.acell(STATUS_CELL).value
            if current_status and "Error" in current_status:
                if new_trigger_value == "Start":
                    trigger_sheet.update_acell(STATUS_CELL, "Running")
                    print("Fixed error status to Running")
                else:
                    trigger_sheet.update_acell(STATUS_CELL, "Ready")
                    print("Fixed error status to Ready")
            
            # If value changed
            if new_trigger_value != current_trigger_value:
                print(f"Trigger changed from '{current_trigger_value}' to '{new_trigger_value}'")
                current_trigger_value = new_trigger_value
                
                if new_trigger_value == "Start":
                    # Update status
                    trigger_sheet.update_acell(STATUS_CELL, "Running")
                    
                    # Start the scraping process
                    start_scraper()
                
                elif new_trigger_value == "Stop":
                    # Update status
                    trigger_sheet.update_acell(STATUS_CELL, "Stopped")
                    
                    # Stop the scraping process
                    stop_scraper()
            
            # Check if process has completed on its own - SIMPLIFIED VERSION
            if scraper_process is not None:
                return_code = scraper_process.poll()
                if return_code is not None:
                    # Process has terminated - just log it but don't update sheet with error info
                    print(f"Process terminated with code: {return_code}")
                    
                    # If trigger is still Start, restart the process and keep status as Running
                    if current_trigger_value == "Start":
                        print("Process ended but trigger is still Start. Restarting.")
                        trigger_sheet.update_acell(STATUS_CELL, "Running")
                        start_scraper()
                    else:
                        # If trigger is not Start, show Completed
                        trigger_sheet.update_acell(STATUS_CELL, "Completed")
                        scraper_process = None
            
            # Wait before checking again
            time.sleep(CHECK_INTERVAL)
            
        except Exception as e:
            print(f"Error in monitor loop: {e}")
            time.sleep(60)  # Longer delay if there's an error
            
            # Try to reinitialize the client
            try:
                sheets_client = initialize_sheets_client()
                trigger_sheet = get_trigger_sheet(sheets_client)
            except Exception as reinit_error:
                print(f"Failed to reinitialize sheets client: {reinit_error}")


def handle_exit(signum, frame):
    """Handle termination signals"""
    print("Received termination signal, shutting down...")
    if scraper_process is not None:
        stop_scraper()
    sys.exit(0)


if __name__ == "__main__":
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)
    
    try:
        monitor_sheet()
    except KeyboardInterrupt:
        print("Monitor interrupted by user")
        if scraper_process is not None:
            stop_scraper()
    except Exception as e:
        print(f"Fatal error in monitor: {e}")
        if scraper_process is not None:
            stop_scraper()