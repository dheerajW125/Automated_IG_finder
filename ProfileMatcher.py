"""
AI-based Profile Matcher
Evaluates Instagram profiles to find best match for a person
"""
import json
import time
import re
import google.generativeai as genai

class ProfileMatcher:
    def __init__(self, gemini_api_key='AIzaSyA8-o0dWnYCGvQ9X7Nu1PFv9kpHe2ISeHg'):
        # Initialize Gemini AI for profile evaluation
        self.gemini_api_key = gemini_api_key
        genai.configure(api_key=self.gemini_api_key)
        self.gemini_model = genai.GenerativeModel('gemini-2.0-flash')
        
        # API call counter
        self.api_calls = 0
    
    def evaluate_profiles_with_gemini(self, name, location, usernames_with_metadata):
        """Use Gemini AI to evaluate and rank Instagram usernames for a person with provided metadata"""
        if not usernames_with_metadata:
            return {"best_match": "No match found", "confidence_score": 0, "ranked_usernames": []}
        
        try:
            usernames = list(usernames_with_metadata.keys())
            print(f"Using Gemini AI to evaluate {len(usernames)} profiles for {name}")
            self.api_calls += 1
            
            # Construct prompt for Gemini with profile metadata
            prompt = f"""
            I need to find the Instagram profile that most likely belongs to a person with these details:
            Name: {name}
            Location: {location}
            
            Here are the potential Instagram profiles found through search, with their metadata:
            {json.dumps(usernames_with_metadata, indent=2)}
            
            Please analyze these profiles and determine which one most likely belongs to the person.
            Consider factors like:
            - Similarity between the profile's full_name and the person's name
            - Whether the location information appears in the profile's biography or other fields
            - Profile characteristics that suggest it's a personal rather than business account
            - If the biography contains professional details that might relate to the person
            - Whether follower/following counts and post counts suggest a real person's profile
            - Profile verification status and category
            - If email addresses in the profile match or are similar to the person's name
            - Content of raw_bio or biography fields that might confirm identity
            
            Return your answer in JSON format with these fields:
            1. best_match: The username that is most likely the correct match
            2. confidence_score: A score from 0-100 indicating your confidence level
            3. ranked_usernames: A list of all usernames ranked from most to least likely
            4. reasoning: A brief explanation of why you selected the best match
            
            Only return the JSON object, nothing else.
            """
            
            # Make the API call to Gemini
            response = self.gemini_model.generate_content(prompt)
            result_text = response.text
            
            # Extract JSON from the response
            try:
                # First, check if the response is already valid JSON
                try:
                    result_json = json.loads(result_text)
                except json.JSONDecodeError:
                    # If not, try to extract JSON from markdown code block
                    json_match = re.search(r'```(?:json)?\s*(.*?)\s*```', result_text, re.DOTALL)
                    if json_match:
                        result_json = json.loads(json_match.group(1))
                    else:
                        # Try to extract anything that looks like JSON
                        json_match = re.search(r'(\{.*\})', result_text, re.DOTALL)
                        if json_match:
                            result_json = json.loads(json_match.group(1))
                        else:
                            raise ValueError("Could not extract JSON from Gemini response")
                
                print(f"Gemini AI selected {result_json.get('best_match')} with confidence {result_json.get('confidence_score')}")
                return result_json
            except Exception as e:
                print(f"Error parsing Gemini response: {e}")
                print(f"Raw response text: {result_text}")
                # Fallback to simple matching if Gemini fails
                return {
                    "best_match": usernames[0] if usernames else "No match found",
                    "confidence_score": 0,
                    "ranked_usernames": usernames,
                    "reasoning": "Fallback due to Gemini parsing error"
                }
                
        except Exception as e:
            print(f"Error using Gemini AI: {e}")
            # Fallback to basic matching
            return {
                "best_match": list(usernames_with_metadata.keys())[0] if usernames_with_metadata else "No match found",
                "confidence_score": 0,
                "ranked_usernames": list(usernames_with_metadata.keys()),
                "reasoning": "Gemini AI error, using fallback"
            }