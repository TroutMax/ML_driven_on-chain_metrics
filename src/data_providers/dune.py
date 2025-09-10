"""
Dune Analytics API Client
"""

import os
import time
from typing import Dict, Optional
import pandas as pd
from dune_client.client import DuneClient
from .base import BaseDataProvider

class DuneProvider(BaseDataProvider):
    """
    Dune Analytics data provider
    Handles blockchain data queries through Dune's SQL interface
    """
    
    def __init__(self, api_key: Optional[str] = None):
        # Get API key from environment if not provided
        if api_key is None:
            api_key = os.getenv('DUNE_API_KEY')
        
        super().__init__(api_key=api_key, rate_limit=30)  # Dune allows ~30 requests/minute
        
        if not self.api_key:
            raise ValueError("Dune API key is required. Set DUNE_API_KEY environment variable.")
        
        self.client = DuneClient(self.api_key)
        self.query_cache = {}  # Simple cache for queries
        self.cache_duration = 3600  # 1 hour cache
        
        # Known query mappings
        self.query_mappings = {
            'bot_volume': 5745512,  # Your existing query
            # Add more query IDs as you find them
        }
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Dune client handles auth internally"""
        return {'X-Dune-API-Key': self.api_key}
    
    def _get_base_url(self) -> str:
        return "https://api.dune.com"
    
    def get_query_result(self, query_id: int, use_cache: bool = True) -> pd.DataFrame:
        """
        Execute a Dune query and return results as DataFrame
        
        Args:
            query_id: Dune query ID
            use_cache: Whether to use cached results
        """
        cache_key = f"query_{query_id}"
        
        # Check cache first
        if use_cache and cache_key in self.query_cache:
            cached_time, cached_data = self.query_cache[cache_key]
            if time.time() - cached_time < self.cache_duration:
                self.logger.info(f"Using cached data for query {query_id}")
                return cached_data
        
        # Fetch fresh data
        self.logger.info(f"Fetching fresh data for query {query_id}")
        try:
            df = self.client.get_latest_result_dataframe(query_id)
            
            # Cache the result
            self.query_cache[cache_key] = (time.time(), df)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to fetch query {query_id}: {e}")
            raise
    
    def get_market_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        """
        Get market data (implementation depends on your specific queries)
        For now, returns bot volume data as an example
        """
        return self.get_bot_volume_data()
    
    def get_bot_volume_data(self) -> pd.DataFrame:
        """Get trading bot volume data across blockchains"""
        query_id = self.query_mappings['bot_volume']
        return self.get_query_result(query_id)
    
    def validate_connection(self) -> bool:
        """Test if Dune API connection is working"""
        try:
            # Try to fetch a small query result
            df = self.get_query_result(self.query_mappings['bot_volume'])
            return len(df) > 0
        except:
            return False
    
    def get_available_queries(self) -> Dict[str, int]:
        """Get available query mappings"""
        return self.query_mappings.copy()
