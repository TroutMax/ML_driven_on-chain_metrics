"""
Hyperliquid API Client
"""

import time
import pandas as pd
from typing import Dict, Any, Optional, List
from .base import BaseDataProvider

class HyperliquidProvider(BaseDataProvider):
    """Hyperliquid exchange data provider"""
    
    def __init__(self, api_key: Optional[str] = None):
        # Hyperliquid typically has higher rate limits
        super().__init__(api_key=api_key, rate_limit=100)  # 100 requests per minute
        
        # Hyperliquid endpoints
        self.base_url = "https://api.hyperliquid.xyz"
        self.endpoints = {
            'info': '/info',
            'meta': '/info/meta',
            'userFills': '/info/userFills',
            'candlestick': '/info/candlestick',
            'recentTrades': '/info/recentTrades',
            'userOpenOrders': '/info/openOrders',
            'userState': '/info/userState',
            'funding': '/info/funding',
            'levels': '/info/levels'
        }
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Hyperliquid auth headers - update based on their auth requirements"""
        headers = {'Content-Type': 'application/json'}
        if self.api_key:
            headers['Authorization'] = f'Bearer {self.api_key}'
        return headers
    
    def _get_base_url(self) -> str:
        return self.base_url
    
    def get_market_data(self, symbol: str = "ETH", interval: str = "1h", 
                       start_time: Optional[int] = None, end_time: Optional[int] = None) -> pd.DataFrame:
        """Get candlestick market data"""
        params = {
            "coin": symbol,
            "interval": interval
        }
        
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        
        response = self._make_request('GET', self.endpoints['candlestick'], params=params)
        
        if response and isinstance(response, list):
            df = pd.DataFrame(response)
            if not df.empty:
                # Standard OHLCV column naming
                df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df = df.set_index('timestamp')
                
                # Convert to numeric
                numeric_cols = ['open', 'high', 'low', 'close', 'volume']
                for col in numeric_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
            return df
        
        return pd.DataFrame()
    
    def get_user_fills(self, user_address: str, start_time: Optional[int] = None) -> pd.DataFrame:
        """Get user trading history"""
        params = {"user": user_address}
        if start_time:
            params["startTime"] = start_time
        
        response = self._make_request('POST', self.endpoints['userFills'], 
                                    json_data=params)
        
        if response:
            df = pd.DataFrame(response)
            if not df.empty:
                # Convert timestamp columns
                if 'time' in df.columns:
                    df['time'] = pd.to_datetime(df['time'], unit='ms')
                    
                # Convert numeric columns
                numeric_cols = ['px', 'sz', 'fee']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        
            return df
        
        return pd.DataFrame()
    
    def get_user_state(self, user_address: str) -> Dict[str, Any]:
        """Get current user state (positions, balances, etc.)"""
        params = {"user": user_address}
        
        response = self._make_request('POST', self.endpoints['userState'], 
                                    json_data=params)
        return response or {}
    
    def get_funding_rates(self, coin: str = "ETH") -> pd.DataFrame:
        """Get funding rate history"""
        params = {"coin": coin}
        
        response = self._make_request('POST', self.endpoints['funding'], 
                                    json_data=params)
        
        if response:
            df = pd.DataFrame(response)
            if not df.empty and 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'], unit='ms')
                if 'fundingRate' in df.columns:
                    df['fundingRate'] = pd.to_numeric(df['fundingRate'], errors='coerce')
            return df
        
        return pd.DataFrame()
    
    def get_order_book(self, coin: str = "ETH") -> Dict[str, Any]:
        """Get current order book levels"""
        params = {"coin": coin}
        
        response = self._make_request('POST', self.endpoints['levels'], 
                                    json_data=params)
        return response or {}
    
    def get_recent_trades(self, coin: str = "ETH") -> pd.DataFrame:
        """Get recent trades"""
        params = {"coin": coin}
        
        response = self._make_request('POST', self.endpoints['recentTrades'], 
                                    json_data=params)
        
        if response:
            df = pd.DataFrame(response)
            if not df.empty:
                if 'time' in df.columns:
                    df['time'] = pd.to_datetime(df['time'], unit='ms')
                    
                numeric_cols = ['px', 'sz']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        
            return df
        
        return pd.DataFrame()
    
    def validate_connection(self) -> bool:
        """Test Hyperliquid API connection"""
        try:
            # Try to fetch meta info (public endpoint)
            response = self._make_request('GET', self.endpoints['meta'])
            return response is not None
        except Exception as e:
            self.logger.error(f"Hyperliquid connection validation failed: {e}")
            return False
    
    def get_all_coins_info(self) -> Dict[str, Any]:
        """Get information about all available coins"""
        response = self._make_request('GET', self.endpoints['meta'])
        return response or {}
