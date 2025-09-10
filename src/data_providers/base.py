"""
Base Data Provider Class
All API clients inherit from this for consistency
"""

from abc import ABC, abstractmethod
import time
import logging
import requests
from typing import Dict, Any, Optional
import pandas as pd

class BaseDataProvider(ABC):
    """
    Abstract base class for all data providers
    Handles rate limiting, authentication, and common functionality
    """
    
    def __init__(self, api_key: Optional[str] = None, rate_limit: int = 60):
        self.api_key = api_key
        self.rate_limit = rate_limit  # requests per minute
        self.last_request_time = 0
        self.request_count = 0
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
    
    @abstractmethod
    def _get_auth_headers(self) -> Dict[str, str]:
        """Return authentication headers for API requests"""
        pass
    
    @abstractmethod
    def _get_base_url(self) -> str:
        """Return the base URL for the API"""
        pass
    
    @abstractmethod
    def get_market_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        """Get market data for a specific symbol"""
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """Test if the API connection is working"""
        pass
    
    def _rate_limit_wait(self):
        """Ensure we don't exceed rate limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 60 / self.rate_limit  # seconds between requests
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            self.logger.info(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    def _make_request(self, method: str, endpoint: str, params: Dict = None, data: Dict = None) -> Dict:
        """Make a rate-limited HTTP request"""
        self._rate_limit_wait()
        
        url = f"{self._get_base_url()}{endpoint}"
        headers = self._get_auth_headers()
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            raise
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get provider health information"""
        return {
            'provider': self.__class__.__name__,
            'total_requests': self.request_count,
            'rate_limit': self.rate_limit,
            'connection_valid': self.validate_connection()
        }
