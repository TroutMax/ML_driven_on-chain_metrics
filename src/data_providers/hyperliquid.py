"""
Hyperliquid API Client
"""

from typing import Dict, Optional, List
import pandas as pd
from datetime import datetime
from .base import BaseDataProvider

class HyperliquidProvider(BaseDataProvider):
    """
    Hyperliquid DEX data provider
    Access to perpetual futures data, funding rates, and trading data
    """
    
    def __init__(self, api_key: Optional[str] = None):
        # Hyperliquid public data doesn't require API key
        super().__init__(api_key=api_key, rate_limit=100)  # Conservative rate limit
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Hyperliquid public endpoints don't require auth"""
        return {'Content-Type': 'application/json'}
    
    def _get_base_url(self) -> str:
        return "https://api.hyperliquid.xyz"
    
    def get_market_data(self, symbol: str = 'ETH', interval: str = '1h') -> pd.DataFrame:
        """
        Get OHLCV market data for a symbol
        
        Args:
            symbol: Trading symbol (e.g., 'ETH', 'BTC')
            interval: Time interval ('1m', '5m', '1h', '1d')
        """
        try:
            # Hyperliquid candlestick endpoint
            payload = {
                "type": "candleSnapshot",
                "req": {
                    "coin": symbol,
                    "interval": interval,
                    "startTime": int((datetime.now().timestamp() - 86400) * 1000)  # Last 24h
                }
            }
            
            response = self._make_request('POST', '/info', data=payload)
            
            if not response or 'data' not in response:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(response['data'])
            
            if df.empty:
                return df
            
            # Standardize column names
            df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            
            # Convert timestamp from milliseconds
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # Convert price/volume columns to numeric
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Set timestamp as index
            df = df.set_index('timestamp')
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to get market data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_funding_rates(self, symbol: str = 'ETH') -> pd.DataFrame:
        """Get funding rate data for perpetual futures"""
        try:
            payload = {
                "type": "funding",
                "coin": symbol
            }
            
            response = self._make_request('POST', '/info', data=payload)
            
            if not response:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame([response])
            df['timestamp'] = pd.to_datetime(datetime.now())
            df['symbol'] = symbol
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to get funding rates for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_recent_trades(self, symbol: str = 'ETH', limit: int = 100) -> pd.DataFrame:
        """Get recent trades for a symbol"""
        try:
            payload = {
                "type": "recentTrades",
                "coin": symbol
            }
            
            response = self._make_request('POST', '/info', data=payload)
            
            if not response:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(response[:limit])  # Limit results
            
            if df.empty:
                return df
            
            # Standardize columns
            if 'time' in df.columns:
                df['timestamp'] = pd.to_datetime(df['time'], unit='ms')
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to get recent trades for {symbol}: {e}")
            return pd.DataFrame()
    
    def validate_connection(self) -> bool:
        """Test if Hyperliquid API is accessible"""
        try:
            # Try to get market data for ETH
            df = self.get_market_data('ETH')
            return not df.empty
        except:
            return False
    
    def get_available_symbols(self) -> List[str]:
        """Get list of available trading symbols"""
        try:
            payload = {"type": "meta"}
            response = self._make_request('POST', '/info', data=payload)
            
            if response and 'universe' in response:
                return [asset['name'] for asset in response['universe']]
            
            return ['ETH', 'BTC']  # Fallback
            
        except:
            return ['ETH', 'BTC']  # Fallback
