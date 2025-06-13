"""
Google Sheets integration for input/output operations
Handles reading from and writing to Google Sheets
"""
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

class SheetsHandler:
    def __init__(self, credentials_file='credentials.json'):
        """Initialize the Google Sheets client"""
        # Initialize Google Sheets client
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        self.sheets_client = gspread.authorize(credentials)
    
    def get_available_sheets(self):
        """List all accessible Google Sheets"""
        return self.sheets_client.openall()
    
    def open_sheet_by_url(self, sheet_url):
        """Open a specific spreadsheet by URL"""
        try:
            sheet = self.sheets_client.open_by_url(sheet_url)
            return sheet, sheet.title
        except Exception as e:
            print(f"Could not open specified sheet, will try available sheets: {e}")
            return None, None
    
    def load_data_from_sheet(self, sheet_name, worksheet_name):
        """Load people data from Google Sheet"""
        try:
            # Try to open the spreadsheet by name
            try:
                sheet = self.sheets_client.open(sheet_name)
            except gspread.exceptions.SpreadsheetNotFound:
                sheets = self.get_available_sheets()
                if not sheets:
                    print("No accessible spreadsheets found.")
                    return []
                sheet = sheets[0]
                print(f"Using spreadsheet: {sheet.title}")
            
            # Try to get the specific worksheet
            try:
                worksheet = sheet.worksheet(worksheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = sheet.get_worksheet(0)
                print(f"Using worksheet: {worksheet.title}")
            
            # Get all values and headers
            all_values = worksheet.get_all_values()
            if not all_values:
                print("Worksheet is empty.")
                return []
                
            headers = all_values[0]
            print(f"Headers detected: {headers}")
            
            # Find column indexes
            col_indexes = {}
            required_columns = ['name', 'email', 'location']
            for col in required_columns:
                col_indexes[col] = next((i for i, h in enumerate(headers) 
                                        if h.lower() == col.lower() or h == col.capitalize()), None)
                if col_indexes[col] is None:
                    print(f"Required column '{col}' not found in headers.")
                    return []
            
            # Check if status column exists, if not, add it
            status_col_index = next((i for i, h in enumerate(headers) 
                                   if h.lower() == 'status'), None)
            if status_col_index is None:
                # Add status column to the sheet
                headers.append('status')
                worksheet.update_cell(1, len(headers), 'status')
                status_col_index = len(headers) - 1
                print("Added 'status' column to track processing progress")
            
            # Convert to list of dictionaries
            records = []
            for i, row in enumerate(all_values[1:], 2):  # Start from 2 to account for header row
                if row and any(row):  # Skip empty rows
                    # Get status if available
                    status = row[status_col_index] if status_col_index < len(row) else ""
                    
                    # Skip already processed rows
                    if status == "complete":
                        print(f"Skipping row {i} (already processed)")
                        continue
                    
                    record = {
                        'name': row[col_indexes['name']],
                        'email': row[col_indexes['email']],
                        'location': row[col_indexes['location']],
                        'row_index': i,  # Store the row index for updating status
                        'status_col_index': status_col_index + 1  # Convert to 1-indexed for worksheet.update_cell
                    }
                    records.append(record)
            
            print(f"Successfully loaded {len(records)} records to process from Google Sheet")
            return records
        except Exception as e:
            print(f"Error loading data from Google Sheet: {e}")
            return []
    
    def update_status(self, sheet_name, worksheet_name, row_index, status_col_index, status):
        """Update the status of a row in the input sheet"""
        try:
            sheet = self.sheets_client.open(sheet_name)
            worksheet = sheet.worksheet(worksheet_name)
            worksheet.update_cell(row_index, status_col_index, status)
            print(f"Updated status for row {row_index} to '{status}'")
            return True
        except Exception as e:
            print(f"Error updating status: {e}")
            return False
    
    def initialize_results_worksheet(self, sheet_name, results_worksheet):
        """Initialize or get the existing results worksheet"""
        try:
            sheet = self.sheets_client.open(sheet_name)
            existing_results = {}
            
            try:
                # Try to get the existing worksheet
                results_sheet = sheet.worksheet(results_worksheet)
                
                # Get existing results to support resuming
                all_rows = results_sheet.get_all_values()
                if len(all_rows) > 1:  # If there's data beyond the header row
                    headers = all_rows[0]
                    name_index = headers.index("name") if "name" in headers else 0
                    
                    # Store existing results by name to avoid duplicates
                    for row in all_rows[1:]:  # Skip header row
                        if len(row) > name_index:
                            existing_results[row[name_index]] = True
                    
                    print(f"Found {len(existing_results)} existing results in output sheet")
                
            except gspread.exceptions.WorksheetNotFound:
                # Create a new worksheet if it doesn't exist
                results_sheet = sheet.add_worksheet(title=results_worksheet, rows=1000, cols=23)
                
                # Add header row
                header_row = [
                    "name", "email", "location", "best_match", "all_potential_matches",
                    "follower_count", "following_count", "media_count", "biography", 
                    "category", "is_verified", "bio_usernames", "profile_url", 
                    "external_url", "category_id", "is_business", "contact_number", "public_email",
                    "is_influencer", "gemini_confidence_score", "gemini_reasoning", "metadata_source", "processing_time"  
                ]
                results_sheet.append_row(header_row)
                print(f"Created new results worksheet: {results_worksheet}")
            
            return results_sheet, existing_results
            
        except Exception as e:
            print(f"Error initializing results worksheet: {e}")
            return None, {}
    
    def add_result_to_sheet(self, results_sheet, person_result):
        """Add a result to the results worksheet"""
        if not results_sheet:
            return False
            
        try:
            # Extract data
            person = person_result["person_info"]
            name = person["name"]
            best_match = person_result["best_match"]
            insta_data = person_result.get("instagram_data", {})
            
            # Process top matches
            top_matches = ", ".join(person_result["ranked_usernames"][:5]) if person_result["ranked_usernames"] else ""
            
            # Extract usernames from biography_with_entities
            bio_with_entities = insta_data.get("biography_with_entities", {})
            bio_usernames = []
            
            if bio_with_entities and isinstance(bio_with_entities, dict):
                entities = bio_with_entities.get("entities", [])
                if entities and isinstance(entities, list):
                    for entity in entities:
                        if isinstance(entity, dict) and "user" in entity:
                            user = entity.get("user", {})
                            if "username" in user:
                                bio_usernames.append(user["username"])
            
            bio_usernames_str = ", ".join(bio_usernames) if bio_usernames else ""
            
            # Calculate is_influencer
            follower_count = insta_data.get("follower_count", 0)
            try:
                if isinstance(follower_count, str) and follower_count.isdigit():
                    follower_count = int(follower_count)
                is_influencer = "true" if follower_count > 5000 else "false"
            except (ValueError, TypeError):
                is_influencer = "false"
            
            # Get metadata source
            metadata_source = insta_data.get("metadata_source", "unknown")
            
            # Create the row to add
            row = [
                name,
                person["email"],
                person["location"],
                best_match,
                top_matches,
                insta_data.get("follower_count", ""),
                insta_data.get("following_count", ""),
                insta_data.get("media_count", ""),
                insta_data.get("biography", ""),
                insta_data.get("category", ""),
                insta_data.get("is_verified", ""),
                bio_usernames_str,
                insta_data.get("profile_pic_url_hd", ""),
                insta_data.get("external_url", ""),
                insta_data.get("category_id", ""),
                insta_data.get("is_business", ""),
                insta_data.get("contact_phone_number", ""),
                insta_data.get("public_email", ""),
                is_influencer,
                person_result["gemini_confidence_score"],
                person_result.get("gemini_reasoning", ""),
                metadata_source,
                person_result["processing_time"]
            ]
            
            # Add to sheet
            results_sheet.append_row(row)
            print(f"Added result for {name} to output sheet in real-time")
            return True
            
        except Exception as e:
            print(f"Error updating results sheet for {name}: {e}")
            return False