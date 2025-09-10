"""
Base Data Provider Class
All API clients inherit from this for consistency
"""

import requests
import pandas as pd
from abc import ABC, abstractmethod
import time
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

class BaseDataProvider(ABC):
    """Base class for all data providers"""
    
    def __init__(self, api_key: Optional[str] = None, rate_limit: int = 60):
        self.api_key = api_key
        self.rate_limit = rate_limit  # requests per minute
        self.last_request_time = 0
        self.session = requests.Session()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Set up session headers if API key provided
        if api_key:
            self.session.headers.update(self._get_auth_headers())
    
    @abstractmethod
    def _get_auth_headers(self) -> Dict[str, str]:
        """Return authentication headers for API requests"""
        pass
    
    @abstractmethod
    def _get_base_url(self) -> str:
        """Return the base URL for the API"""
        pass
    
    def _rate_limit_wait(self):
        """Ensure we don't exceed rate limits"""
        time_since_last = time.time() - self.last_request_time
        min_interval = 60 / self.rate_limit  # seconds between requests
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Dict = None, method: str = 'GET') -> Dict[str, Any]:
        """Make a rate-limited API request"""
        self._rate_limit_wait()
        
        url = f"{self._get_base_url()}{endpoint}"
        
        try:
            if method.upper() == 'GET':
                response = self.session.get(url, params=params)
            elif method.upper() == 'POST':
                response = self.session.post(url, json=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            raise
    
    @abstractmethod
    def get_market_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        """Get market data for a symbol"""
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """Test if API connection is working"""
        pass
    
    def get_supported_symbols(self) -> List[str]:
        """Get list of supported trading symbols"""
        return []
    
    def health_check(self) -> Dict[str, Any]:
        """Return provider health status"""
        try:
            is_connected = self.validate_connection()
            return {
                'provider': self.__class__.__name__,
                'status': 'healthy' if is_connected else 'unhealthy',
                'last_check': datetime.now().isoformat(),
                'rate_limit': self.rate_limit
            }
        except Exception as e:
            return {
                'provider': self.__class__.__name__,
                'status': 'error',
                'error': str(e),
                'last_check': datetime.now().isoformat()
            }
