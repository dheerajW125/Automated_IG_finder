"""
Main application module - Instagram Finder
Fully optimized version with BrightData SERP proxy support
"""
import time
import os
import urllib3
from sheets_handler import SheetsHandler
from InstagramDataExtractor import InstagramDataExtractor
from ProfileMatcher import ProfileMatcher
from dotenv import load_dotenv

# Disable SSL warnings for proxy connections
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables if .env file exists
load_dotenv()

class InstagramFinder:
    def __init__(self, 
                 credentials_file='credentials.json', 
                 rapidapi_key=None,
                 brightdata_key=None,
                 gemini_api_key=None):
        """Initialize the main components of the Instagram Finder"""
        
        # Set default API keys if not provided
        self.rapidapi_key = rapidapi_key or os.getenv("RAPIDAPI_KEY", "Your API Key")
        self.brightdata_key = brightdata_key  # This is now optional since we're not using the API
        self.gemini_api_key = gemini_api_key or os.getenv("GEMINI_API_KEY", "Your Gemini Key")
        
        # Print proxy configuration being used
        print(f"Using BrightData SERP proxy: {os.getenv('SERP_HOST', 'brd.superproxy.io')}:{os.getenv('SERP_PORT', '33335')}")
        print(f"Using SERP proxy user: {os.getenv('SERP_USER', 'SERP_USER_ID')}")
        
        # Initialize each component
        self.sheets_handler = SheetsHandler(credentials_file)
        self.insta_extractor = InstagramDataExtractor(self.rapidapi_key)  # No BrightData API key needed
        self.profile_matcher = ProfileMatcher(self.gemini_api_key)
        
        # Performance metrics tracking
        self.processing_stats = {
            'total_people_processed': 0,
            'successful_matches': 0,
            'high_confidence_matches': 0,
            'gemini_calls': 0,
            'brightdata_calls': 0,
            'rapidapi_calls': 0
        }
    
    def find_instagram_profiles(self, people_data, sheet_name, input_worksheet, results_worksheet):
        """Main method to find Instagram profiles for a list of people"""
        all_results = {}
        
        # Initialize or get the existing results worksheet
        results_sheet, existing_results = self.sheets_handler.initialize_results_worksheet(
            sheet_name, results_worksheet
        )
        
        for person in people_data:
            start_time = time.time()
            name = person["name"]
            location = person["location"]
            email = person.get("email", "")  # Get email if available
            row_index = person["row_index"]
            status_col_index = person["status_col_index"]
            
            # Skip if this person is already in the results sheet (for resuming)
            if name in existing_results:
                print(f"Skipping {name} - already exists in results sheet")
                continue
                
            print(f"\nProcessing: {name} from {location} (Row {row_index})")
            
            try:
                # 1. Update status to "processing"
                self.sheets_handler.update_status(sheet_name, input_worksheet, row_index, status_col_index, "processing")
                
                # 2. Initialize results structure
                person_result = {
                    "person_info": person,
                    "all_usernames": [],
                    "metadata": {}
                }
                
                # 3. Execute the fully optimized search using BrightData SERP proxy
                usernames, metadata_dict, query_results = self.insta_extractor.batch_search_instagram_profiles(name, location, email)
                
                # Track BrightData calls
                self.processing_stats['brightdata_calls'] += 1
                
                # 4. Store search results
                person_result["all_usernames"] = usernames
                person_result["metadata"] = metadata_dict
                person_result["queries"] = query_results
                
                # 5. If we found any usernames, use Gemini AI to evaluate profiles
                if usernames:
                    # Track Gemini call
                    self.processing_stats['gemini_calls'] += 1
                    
                    # Make a single Gemini API call
                    gemini_result = self.profile_matcher.evaluate_profiles_with_gemini(name, location, metadata_dict)
                    
                    # 6. Save Gemini's evaluation in the results
                    person_result["best_match"] = gemini_result.get("best_match", "No match found")
                    person_result["gemini_confidence_score"] = gemini_result.get("confidence_score", 0)
                    person_result["ranked_usernames"] = gemini_result.get("ranked_usernames", [])
                    person_result["gemini_reasoning"] = gemini_result.get("reasoning", "")
                    
                    # 7. Get detailed Instagram profile data for the best match ONLY
                    best_match = person_result["best_match"]
                    confidence_score = gemini_result.get("confidence_score", 0)
                    
                    if best_match and best_match != "No match found":
                        # Make a single RapidAPI call only for the final best match
                        insta_data = self.insta_extractor.get_instagram_data(best_match, confidence_score, name)
                        
                        # Track RapidAPI call if made
                        if insta_data.get('metadata_source') == 'rapidapi':
                            self.processing_stats['rapidapi_calls'] += 1
                            
                        person_result["instagram_data"] = insta_data
                        print(f"Got Instagram data for {best_match} (confidence: {confidence_score})")
                        
                        # Update stats
                        self.processing_stats['successful_matches'] += 1
                        if confidence_score >= 70:
                            self.processing_stats['high_confidence_matches'] += 1
                    else:
                        person_result["instagram_data"] = {}
                else:
                    # No usernames found, set default values
                    person_result["best_match"] = "No match found"
                    person_result["gemini_confidence_score"] = 0
                    person_result["ranked_usernames"] = []
                    person_result["gemini_reasoning"] = "No Instagram profiles found for this name and location."
                    person_result["instagram_data"] = {}
                
                # 8. Update status to complete after successful processing
                self.sheets_handler.update_status(sheet_name, input_worksheet, row_index, status_col_index, "complete")
                
                # 9. Store in all_results
                all_results[name] = person_result
                
                # 10. Calculate processing time
                processing_time = time.time() - start_time
                person_result["processing_time"] = f"{processing_time:.2f} seconds"
                
                # 11. Real-time update to results sheet
                if results_sheet:
                    self.sheets_handler.add_result_to_sheet(results_sheet, person_result)
                
                # Update total processed count
                self.processing_stats['total_people_processed'] += 1
                
                # Add a small delay between profiles to avoid rate limits
                if person != people_data[-1]:  # If not the last person
                    delay = os.getenv("PROFILE_DELAY", "3")
                    try:
                        delay = int(delay)
                    except ValueError:
                        delay = 3
                    if delay > 0:
                        print(f"Waiting {delay} seconds before processing next profile...")
                        time.sleep(delay)
                
            except Exception as e:
                print(f"Error processing {name}: {e}")
                # Update status to error
                self.sheets_handler.update_status(sheet_name, input_worksheet, row_index, status_col_index, "error occurred")
                continue  # Move to the next person
        
        # Print API usage statistics
        print("\nAPI Call Statistics:")
        print(f"  BrightData SERP Calls: {self.processing_stats['brightdata_calls']} calls")
        print(f"  RapidAPI Calls: {self.processing_stats['rapidapi_calls']} calls")
        print(f"  Gemini AI Calls: {self.processing_stats['gemini_calls']} calls")
        
        # Print processing statistics
        print("\nProcessing Statistics:")
        print(f"  Total people processed: {self.processing_stats['total_people_processed']}")
        print(f"  Successful matches: {self.processing_stats['successful_matches']}")
        print(f"  High confidence matches: {self.processing_stats['high_confidence_matches']}")
        
        return all_results


def main():
    # Load environment variables if .env file exists
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("Environment variables loaded from .env file")
    except ImportError:
        print("dotenv package not installed, using default environment variables")
    
    # Define API keys - load from environment variables
    rapidapi_key = os.getenv("RAPIDAPI_KEY", "YOUR RAPIDAPI KEY")
    gemini_api_key = os.getenv("GEMINI_API_KEY", "YOUR GEMINI KEY")
    
    # Initialize the finder with API keys
    finder = InstagramFinder(
        credentials_file='credentials.json',
        rapidapi_key=rapidapi_key,
        gemini_api_key=gemini_api_key
    )
    
    # Define spreadsheet info
    SHEET_NAME = "IG Data"
    INPUT_WORKSHEET = "People Data"
    RESULTS_WORKSHEET = "Search Results"
    
    # Try to open the specific spreadsheet by URL from environment variable or default
    sheet_url = os.getenv("SHEET_URL", "https://docs.google.com/spreadsheets/d/1DWWasUwHx4gn-YKzIAW4zH2I/edit?usp=sharing")
    
    try:
        sheet, sheet_title = finder.sheets_handler.open_sheet_by_url(sheet_url)
        if sheet_title:
            SHEET_NAME = sheet_title
            print(f"Successfully connected to specified Google Sheet: {SHEET_NAME}")
    except Exception as e:
        print(f"Could not open specified sheet, will try available sheets: {e}")
        # List available spreadsheets as fallback
        available_sheets = finder.sheets_handler.get_available_sheets()
        print("Available spreadsheets:")
        for i, sheet in enumerate(available_sheets):
            print(f"  {i+1}. {sheet.title} (ID: {sheet.id})")
        
        if available_sheets and SHEET_NAME not in [s.title for s in available_sheets]:
            SHEET_NAME = available_sheets[0].title
            print(f"Using spreadsheet: {SHEET_NAME}")
    
    # Load people data
    people = finder.sheets_handler.load_data_from_sheet(SHEET_NAME, INPUT_WORKSHEET)
    
    if not people:
        print("No data found or all rows already processed. Make sure your sheet has the correct format.")
        return
    
    # Find Instagram profiles
    finder.find_instagram_profiles(people, SHEET_NAME, INPUT_WORKSHEET, RESULTS_WORKSHEET)
    
    print("\nProcessing complete!")
    print(f"1. Input data sheet: {SHEET_NAME}, worksheet: {INPUT_WORKSHEET}")
    print(f"2. Results sheet updated in real-time: {SHEET_NAME}, worksheet: {RESULTS_WORKSHEET}")


if __name__ == "__main__":
    main()
