#!/usr/bin/python3

import time
import logging
import sys
from functools import wraps

# Configure logging to write to stdout (which Docker will capture)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,  # Explicitly use stdout
    force=True
)
# Configure root logger to ensure all logs are captured
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
# Create our module-specific logger
logger = logging.getLogger('breeze_rate_limiter')

# With this configuration that sets specific log levels
logging.basicConfig(
    level=logging.INFO,  # Set default level to INFO instead of DEBUG
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,  # Explicitly use stdout
    force=True
)
# Set specific logger levels
logging.getLogger('breeze').setLevel(logging.WARNING)  # Set Breeze API library to WARNING level
logging.getLogger('urllib3').setLevel(logging.WARNING)  # Set HTTP client to WARNING level
logging.getLogger('requests').setLevel(logging.WARNING)  # Set requests to WARNING level

# Create our module-specific logger
logger = logging.getLogger('breeze_rate_limiter')
logger.setLevel(logging.INFO)  # Allow INFO messages from our module

# Also ensure we're flushing stdout frequently
import functools
print = functools.partial(print, flush=True)  # Make print flush immediately

# Store the last API call timestamp
last_api_call_time = 0
MIN_DELAY = 3.5  # Minimum delay in seconds between API calls
# Flag to track if rate limiting has been applied
rate_limiting_applied = False
# Global API instance to be shared
global_api_instance = None

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
            logger.info(f"Rate limiting: waiting {sleep_time:.2f} seconds before next API call")
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
    that make HTTP requests. Only applies once regardless of how many times called.
    """
    global rate_limiting_applied
    
    # If rate limiting has already been applied, don't do it again
    if rate_limiting_applied:
        logger.info("Rate limiting already applied to Breeze API")
        return True
    
    try:
        from breeze import breeze
        
        # List of all BreezeApi methods that make HTTP requests
        api_methods = [
            '_request'  # This is the base method that makes all HTTP requests
        ]
        
        # Apply the rate limiting decorator to each method
        for method_name in api_methods:
            if hasattr(breeze.BreezeApi, method_name):
                original_method = getattr(breeze.BreezeApi, method_name)
                setattr(breeze.BreezeApi, method_name, rate_limit_breeze(original_method))
                logger.info(f"Applied rate limiting to BreezeApi.{method_name}")
        
        # Mark rate limiting as applied
        rate_limiting_applied = True
        logger.info("Rate limiting successfully applied to Breeze API")
        return True
        
    except ImportError:
        logger.error("Failed to import Breeze API - rate limiting not applied")
        return False
    except Exception as e:
        logger.error(f"Error applying rate limiting: {e}")
        return False


# Function to get a rate-limited API instance (singleton pattern)
def get_rate_limited_breeze_api():
    """
    Get a rate-limited Breeze API instance
    Returns the same instance every time this is called (singleton pattern)
    """
    global global_api_instance
    
    # If we already have a global instance, return it
    if global_api_instance is not None:
        return global_api_instance
    
    # Otherwise create a new instance
    import os
    from breeze import breeze
    
    api_key = os.environ.get('API_KEY')
    subdomain = 'https://iskconofnc.breezechms.com'
    
    if not api_key:
        logger.error("API_KEY environment variable is not set")
        raise ValueError("API_KEY environment variable is not set")
    
    # Apply rate limiting to the Breeze API class (only happens once)
    apply_rate_limiting_to_breeze()
    
    # Create and save the global instance
    global_api_instance = breeze.BreezeApi(subdomain, api_key)
    logger.info(f"Created Breeze API instance for {subdomain}")
    return global_api_instance


if __name__ == "__main__":
    logger.info("This is a utility module for rate limiting Breeze API calls")
    logger.info("It should be imported and used in your scripts") 