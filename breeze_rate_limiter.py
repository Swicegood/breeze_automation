#!/usr/bin/python3

import time
from functools import wraps

# Store the last API call timestamp
last_api_call_time = 0
MIN_DELAY = 3.5  # Minimum delay in seconds between API calls

def rate_limit_breeze(func):
    """
    Decorator to enforce rate limiting for Breeze API calls
    Ensures at least 3.5 seconds between calls
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        global last_api_call_time
        
        # Calculate time since last API call
        current_time = time.time()
        time_since_last_call = current_time - last_api_call_time
        
        # If we haven't waited enough time, sleep for the remaining time
        if time_since_last_call < MIN_DELAY:
            sleep_time = MIN_DELAY - time_since_last_call
            print(f"Rate limiting: waiting {sleep_time:.2f} seconds before next API call")
            time.sleep(sleep_time)
        
        # Update the last call time and make the API call
        last_api_call_time = time.time()
        result = func(*args, **kwargs)
        
        return result
    
    return wrapper


# Apply rate limiting to Breeze API methods
def apply_rate_limiting_to_breeze():
    """
    Monkey patch the Breeze API to apply rate limiting to all methods
    that make HTTP requests
    """
    try:
        from breeze import breeze
        
        # List of all BreezeApi methods that make HTTP requests
        api_methods = [
            'get_people',
            'get_person_details', 
            'add_person',
            'update_person',
            'list_contributions',
            'add_contribution',
            'list_funds',
            '_request'  # This is the base method that makes all HTTP requests
        ]
        
        # Apply the rate limiting decorator to each method
        for method_name in api_methods:
            if hasattr(breeze.BreezeApi, method_name):
                original_method = getattr(breeze.BreezeApi, method_name)
                setattr(breeze.BreezeApi, method_name, rate_limit_breeze(original_method))
                print(f"Applied rate limiting to BreezeApi.{method_name}")
        
        print("Rate limiting successfully applied to Breeze API")
        return True
        
    except ImportError:
        print("Failed to import Breeze API - rate limiting not applied")
        return False
    except Exception as e:
        print(f"Error applying rate limiting: {e}")
        return False


# Function to get a rate-limited API instance
def get_rate_limited_breeze_api():
    """Get a rate-limited Breeze API instance"""
    import os
    from breeze import breeze
    
    api_key = os.environ.get('API_KEY')
    subdomain = 'https://iskconofnc.breezechms.com'
    
    if not api_key:
        raise ValueError("API_KEY environment variable is not set")
    
    # Apply rate limiting to the Breeze API class
    apply_rate_limiting_to_breeze()
    
    # Create and return a BreezeApi instance
    return breeze.BreezeApi(subdomain, api_key)


if __name__ == "__main__":
    print("This is a utility module for rate limiting Breeze API calls")
    print("It should be imported and used in your scripts") 