"""
Dune Analytics API Client
"""

import os
import pandas as pd
from typing import Dict, Any, Optional
from dune_client.client import DuneClient
from .base import BaseDataProvider

class DuneProvider(BaseDataProvider):
    """Dune Analytics data provider"""
    
    def __init__(self, api_key: Optional[str] = None):
        # Dune has different rate limits
        super().__init__(api_key=api_key, rate_limit=30)  # 30 requests per minute
        
        if not api_key:
            api_key = os.getenv('DUNE_API_KEY')
        
        if not api_key:
            raise ValueError("Dune API key required")
        
        self.client = DuneClient(api_key)
        self.query_cache = {}  # Cache query results
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Dune client handles auth internally"""
        return {}
    
    def _get_base_url(self) -> str:
        """Dune client handles URLs internally"""
        return "https://api.dune.com"
    
    def get_query_result(self, query_id: int, use_cache: bool = True) -> pd.DataFrame:
        """Get results from a Dune query"""
        cache_key = f"query_{query_id}"
        
        if use_cache and cache_key in self.query_cache:
            cache_time, cached_data = self.query_cache[cache_key]
            # Use cache if less than 1 hour old
            if (pd.Timestamp.now() - cache_time).seconds < 3600:
                self.logger.info(f"Using cached data for query {query_id}")
                return cached_data
        
        self.logger.info(f"Fetching fresh data for query {query_id}")
        df = self.client.get_latest_result_dataframe(query_id)
        
        # Cache the result
        self.query_cache[cache_key] = (pd.Timestamp.now(), df)
        
        return df
    
    def get_market_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        """Get market data - implementation depends on your Dune queries"""
        # This would map symbols to specific query IDs
        query_mapping = {
            'bot_volume': 5745512,
            # Add more mappings as you create queries
        }
        
        if symbol in query_mapping:
            return self.get_query_result(query_mapping[symbol])
        else:
            raise ValueError(f"No Dune query mapped for symbol: {symbol}")
    
    def validate_connection(self) -> bool:
        """Test Dune API connection"""
        try:
            # Try to fetch a small query result
            test_df = self.get_query_result(5745512)
            return len(test_df) > 0
        except Exception as e:
            self.logger.error(f"Dune connection validation failed: {e}")
            return False
    
    def get_bot_volume_data(self) -> pd.DataFrame:
        """Get your trading bot volume data"""
        return self.get_query_result(5745512)
    
    def clear_cache(self):
        """Clear query cache"""
        self.query_cache.clear()
        self.logger.info("Query cache cleared")
