"""
Optimized Instagram Data Extractor with BrightData SERP Proxy
Uses BrightData proxies for Google search to find Instagram profiles
"""
import re
import json
import time
import requests
import urllib.parse
from bs4 import BeautifulSoup
import os
from requests.exceptions import RequestException

class InstagramDataExtractor:
    def __init__(self, rapidapi_key='d42cd50256mshf486740ae582110p1946afjsn46b844e85640'):
        # RapidAPI configuration for Instagram profile data
        self.rapidapi_key = rapidapi_key
        self.rapidapi_host = "social-api4.p.rapidapi.com"
        self.rapidapi_url = "https://social-api4.p.rapidapi.com/v1/info"
        
        # BrightData proxy configuration from environment variables
        self.brightdata_proxy_host = os.getenv("SERP_HOST", "brd.superproxy.io")
        self.brightdata_proxy_port = os.getenv("SERP_PORT", "33335")
        self.brightdata_proxy_user = os.getenv("SERP_USER", "brd-customer-hl_4d770a19-zone-serp_api1")
        self.brightdata_proxy_pass = os.getenv("SERP_PASSWORD", "05mk0h7h29hh")
        
        # Instagram system pages to exclude
        self.system_pages = ['explore', 'about', 'developer', 'legal', 'directory', 
                            'p', 'reels', 'stories', 'tv', 'reel', 'story', 'highlights',
                            'direct', 'accounts', 'challenge', 'emails', 'press', 'contact',
                            'tags', 'locations']
        
        # Cache for Instagram metadata
        self.metadata_cache = {}
        
        # API call tracking
        self.api_calls = {
            'brightdata_search': 0,
            'rapidapi': 0
        }
        
        # Track profiles to avoid duplicate processing
        self.current_profile = None
        self.profile_already_processed = set()
    
    def _configure_proxy(self):
        """Configure and return the BrightData proxy settings for SERP"""
        # Create a dynamic session with a random ID
        session_id = f"session-rand{int(time.time())}"
        proxy_url = f"http://{self.brightdata_proxy_user}-{session_id}:{self.brightdata_proxy_pass}@{self.brightdata_proxy_host}:{self.brightdata_proxy_port}"
        
        return {
            "http": proxy_url,
            "https": proxy_url
        }
    
    def batch_search_instagram_profiles(self, name, location="", email=""):
        """Execute a single optimized search and return results"""
        result = self.single_optimized_search(name, location, email)
        return result['usernames'], result['metadata'], [{"query": f"Fully optimized query for {name}", "usernames": result['usernames'], "urls": result['urls']}]
    
    def single_optimized_search(self, name, location="", email=""):
        """Execute a BrightData SERP search for Instagram profiles"""
        # Check if already processed
        profile_key = f"{name}_{location}_{email}"
        if profile_key in self.profile_already_processed:
            print(f"Already processed {name} - using cached data")
            return self.metadata_cache.get(profile_key, {'usernames': [], 'metadata': {}, 'urls': []})
            
        self.profile_already_processed.add(profile_key)
        self.current_profile = profile_key
        
        try:
            # Clean input
            clean_name = re.sub(r'[^\w\s]', '', name).strip()
            clean_location = re.sub(r'[^\w\s]', '', location).strip() if location else ""
            
            # Create optimized search query
            query = f'site:instagram.com {clean_name}'
            if clean_location:
                query += f' {clean_location}'
            query += ' instagram'
            
            print(f"Executing Google search with BrightData SERP proxy for: {name}")
            self.api_calls['brightdata_search'] += 1
            
            # Execute search
            return self._search_with_proxy(query, name, location, email)
                
        except Exception as e:
            print(f"Error searching for '{name}': {e}")
            return {'usernames': [], 'metadata': {}, 'urls': []}
    
    def _search_with_proxy(self, query, name, location, email):
        """Use BrightData SERP proxy to search Google"""
        try:
            # Set SERP parameters
            params = {
                "q": query,
                "num": 20,
                "brd_json": "1",
                "brd_countries": "us",
                "brd_engine": "google",
                "brd_language": "en"
            }
            
            # Try up to 3 times with exponential backoff
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    # Get fresh proxy for each attempt
                    proxies = self._configure_proxy()
                    
                    response = requests.get(
                        'https://www.google.com/search', 
                        params=params, 
                        proxies=proxies, 
                        verify=False,
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        break
                        
                    print(f"Received status code {response.status_code}, retrying...")
                    
                except RequestException as e:
                    print(f"Request attempt {retry_count + 1} failed: {e}")
                
                # Exponential backoff
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count
                    print(f"Waiting {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
            
            if retry_count == max_retries:
                print(f"All {max_retries} attempts failed for {name}")
                return {'usernames': [], 'metadata': {}, 'urls': []}
            
            # Process search results
            try:
                # Try JSON format first
                json_data = response.json()
                print("Successfully received JSON response")
                
                # Handle different JSON structures
                organic_results = json_data.get('organic', [])
                if not organic_results and 'results' in json_data:
                    organic_results = json_data.get('results', {}).get('organic', [])
                
                # Process results
                instagram_profiles = {}
                all_urls = []
                
                for result in organic_results:
                    url = result.get('link', result.get('url', ''))
                    if 'instagram.com/' in url:
                        all_urls.append(url)
                        
                        # Extract username from URL
                        base_url = url.split('?')[0].rstrip('/')
                        match = re.search(r"instagram\.com/([a-zA-Z0-9_.]+)", base_url)
                        if match:
                            username = match.group(1).lower()
                            
                            # Filter valid usernames
                            if username not in self.system_pages and re.match(r'^[a-zA-Z0-9_.]{1,30}$', username):
                                # Extract metadata
                                title = result.get('title', '')
                                snippet = result.get('snippet', result.get('description', ''))
                                
                                metadata = {
                                    "username": username,
                                    "full_name": title.split('|')[0].strip() if '|' in title else '',
                                    "biography": snippet,
                                    "metadata_source": "brightdata_serp_json",
                                    "search_snippet": snippet[:250]
                                }
                                
                                # Name similarity score
                                name_similarity = self.calculate_name_similarity(name, username)
                                if name_similarity > 0:
                                    metadata['name_similarity'] = name_similarity
                                
                                instagram_profiles[username] = metadata
                
                result = {
                    'usernames': list(instagram_profiles.keys()),
                    'metadata': instagram_profiles,
                    'urls': all_urls,
                }
                
                # Cache results
                profile_key = f"{name}_{location}_{email}"
                self.metadata_cache[profile_key] = result
                
                return result
                
            except json.JSONDecodeError:
                # Fall back to HTML processing
                print("Processing HTML response instead of JSON")
                html_content = response.text
                result = self.extract_all_profile_data(html_content, name, location, email)
                
                # Cache results
                profile_key = f"{name}_{location}_{email}"
                self.metadata_cache[profile_key] = result
                
                return result
                
        except RequestException as e:
            print(f"SERP proxy request failed: {e}")
            return {'usernames': [], 'metadata': {}, 'urls': []}
    
    def extract_all_profile_data(self, html_content, name, location, email):
        """Extract Instagram profiles from HTML content"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            instagram_profiles = {}
            all_urls = []
            
            # Find Instagram profile links
            for link in soup.find_all('a', href=True):
                url = link.get('href', '')
                
                if 'instagram.com/' in url and not url.startswith('https://www.google.com'):
                    all_urls.append(url)
                    
                    # Extract username
                    base_url = url.split('?')[0].rstrip('/')
                    match = re.search(r"instagram\.com/([a-zA-Z0-9_.]+)", base_url)
                    if match:
                        username = match.group(1).lower()
                        
                        if username not in self.system_pages and re.match(r'^[a-zA-Z0-9_.]{1,30}$', username):
                            if username not in instagram_profiles:
                                # Get context
                                parent_element = link.find_parent(['div', 'span', 'p'])
                                context_text = parent_element.get_text() if parent_element else link.get_text()
                                
                                # Extract metadata
                                metadata = self.extract_profile_metadata(username, context_text, name, location, email)
                                instagram_profiles[username] = metadata
            
            # Try finding usernames in title tags
            for title_tag in soup.find_all(['h3', 'h2', 'title']):
                title_text = title_tag.get_text()
                if 'instagram' in title_text.lower():
                    username_patterns = [
                        r'@([a-zA-Z0-9_.]{1,30})',
                        r'Instagram:\s+([a-zA-Z0-9_.]{1,30})',
                        r'([a-zA-Z0-9_.]{1,30})\s+on\s+Instagram'
                    ]
                    
                    for pattern in username_patterns:
                        username_match = re.search(pattern, title_text, re.IGNORECASE)
                        if username_match:
                            username = username_match.group(1).lower()
                            if username not in self.system_pages and username not in instagram_profiles:
                                metadata = self.extract_profile_metadata(username, title_text, name, location, email)
                                instagram_profiles[username] = metadata
            
            # Log results
            print(f"Found {len(instagram_profiles)} potential Instagram profiles for {name}")
            if instagram_profiles:
                print(f"Usernames: {', '.join(instagram_profiles.keys())}")
            
            return {
                'usernames': list(instagram_profiles.keys()),
                'metadata': instagram_profiles,
                'urls': all_urls
            }
            
        except Exception as e:
            print(f"Error extracting profiles from HTML: {e}")
            return {'usernames': [], 'metadata': {}, 'urls': []}
    
    def extract_profile_metadata(self, username, context_text, name, location, email):
        """Extract metadata from context around an Instagram profile link"""
        metadata = {
            "username": username,
            "full_name": "",
            "biography": "",
            "follower_count": "",
            "following_count": "",
            "media_count": "",
            "is_verified": "false",
            "is_business": "false",
            "category": "",
            "public_email": "",
            "profile_pic_url_hd": "",
            "external_url": "",
            "metadata_source": "search_result",
            "search_snippet": context_text[:250]
        }
        
        # Follower count
        follower_match = re.search(r'(\d+(?:[,.]\d+)*)\s*(?:followers|Followers)', context_text)
        if follower_match:
            follower_count = follower_match.group(1).replace(',', '').replace('.', '')
            metadata['follower_count'] = follower_count
        
        # Full name
        if '(' in context_text and ')' in context_text:
            name_match = re.search(r'([^(]+)\s*\(@' + re.escape(username) + r'\)', context_text)
            if name_match:
                metadata['full_name'] = name_match.group(1).strip()
        
        # Biography
        bio_match = re.search(r'(?:Bio|Biography):\s*"([^"]+)"', context_text, re.IGNORECASE)
        if bio_match:
            metadata['biography'] = bio_match.group(1).strip()
        elif '"' in context_text:
            quote_match = re.search(r'"([^"]{10,150})"', context_text)
            if quote_match:
                metadata['biography'] = quote_match.group(1).strip()
        
        # Verification status
        if 'verified' in context_text.lower():
            metadata['is_verified'] = 'true'
        
        # Business account indicators
        business_terms = ['business', 'professional', 'company', 'brand', 'official']
        if any(term in context_text.lower() for term in business_terms):
            metadata['is_business'] = 'true'
        
        # Location match
        if location and location.lower() in context_text.lower():
            metadata['location_match'] = 'true'
        
        # Email match
        email_match = re.search(r'([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)', context_text)
        if email_match:
            metadata['public_email'] = email_match.group(1)
        
        if email and email.lower() in context_text.lower():
            metadata['email_match'] = 'true'
            metadata['email_match_score'] = 100
        
        # Name similarity
        name_similarity = self.calculate_name_similarity(name, username)
        if name_similarity > 0:
            metadata['name_similarity'] = name_similarity
        
        return metadata
    
    def calculate_name_similarity(self, search_name, username):
        """Calculate similarity score between search name and username"""
        if not search_name or not username:
            return 0
            
        search_name = search_name.lower()
        username = username.lower()
        
        # Direct match
        if search_name == username:
            return 100
            
        # Clean search name
        clean_search_name = re.sub(r'[^\w]', '', search_name)
        
        # Check if username contains the cleaned search name
        if clean_search_name in username:
            return 90
            
        # Check for partial matches
        search_terms = search_name.split()
        matching_terms = 0
        
        for term in search_terms:
            if len(term) > 3:
                clean_term = re.sub(r'[^\w]', '', term)
                if clean_term in username:
                    matching_terms += 1
        
        # Calculate percentage of matching terms
        if len(search_terms) > 0:
            return int((matching_terms / len(search_terms)) * 80)
            
        return 0
    
    def get_instagram_data(self, username, confidence_score, name):
        """Get detailed Instagram profile data using RapidAPI"""
        # Check cache
        if username in self.metadata_cache:
            return self.metadata_cache[username]
            
        if username == "No match found" or not username:
            return {}
        
        # Use existing metadata if available
        profile_key = self.current_profile
        existing_metadata = {}
        
        if profile_key and profile_key in self.metadata_cache:
            search_result = self.metadata_cache[profile_key]
            if 'metadata' in search_result and username in search_result['metadata']:
                existing_metadata = search_result['metadata'][username]
                
                # Only use RapidAPI for high-confidence matches
                if confidence_score < 50:
                    print(f"Using search metadata for {username} (low confidence: {confidence_score})")
                    return existing_metadata
        
        try:
            print(f"Fetching Instagram data from RapidAPI for: {username}")
            self.api_calls['rapidapi'] += 1
            
            # Prepare API request
            querystring = {"username_or_id_or_url": username}
            headers = {
                "x-rapidapi-key": self.rapidapi_key,
                "x-rapidapi-host": self.rapidapi_host
            }
            
            # Send request with retry logic
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    response = requests.get(self.rapidapi_url, headers=headers, params=querystring, timeout=15)
                    
                    if response.status_code == 200:
                        response_json = response.json()
                        if "data" in response_json:
                            profile_data = response_json["data"]
                            profile_data['metadata_source'] = 'rapidapi'
                            
                            # Cache data
                            self.metadata_cache[username] = profile_data
                            return profile_data
                        else:
                            print(f"API response does not contain 'data' field for {username}")
                            break
                    else:
                        print(f"API status code {response.status_code} for {username}")
                        
                        if response.status_code == 429 or response.status_code >= 500:
                            retry_count += 1
                            wait_time = 2 ** retry_count
                            print(f"Waiting {wait_time} seconds before retrying...")
                            time.sleep(wait_time)
                            continue
                        else:
                            break
                
                except RequestException as e:
                    print(f"Request error: {e}")
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = 2 ** retry_count
                        print(f"Waiting {wait_time} seconds before retrying...")
                        time.sleep(wait_time)
                    continue
            
            # Use existing metadata as fallback
            print(f"Using search metadata for {username} as fallback")
            return existing_metadata
                
        except Exception as e:
            print(f"Error fetching data for '{username}': {e}")
            return existing_metadata