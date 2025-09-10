"""
Hyperliquid API Client - Updated for correct API usage
"""

from typing import Dict, Optional, List
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from .base import BaseDataProvider

class HyperliquidProvider(BaseDataProvider):
    """
    Hyperliquid DEX data provider
    Access to perpetual futures data, funding rates, and trading data
    """
    
    def __init__(self, api_key: Optional[str] = None):
        # Hyperliquid public data doesn't require API key
        super().__init__(api_key=api_key, rate_limit=100)
        self.base_url = "https://api.hyperliquid.xyz"
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Hyperliquid public endpoints don't require auth"""
        return {'Content-Type': 'application/json'}
    
    def _get_base_url(self) -> str:
        return self.base_url
    
    def _make_info_request(self, payload: dict) -> dict:
        """Make a request to the /info endpoint"""
        try:
            headers = self._get_auth_headers()
            response = requests.post(
                f"{self.base_url}/info",
                headers=headers,
                data=json.dumps(payload),
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"API request failed: {e}")
            return {}
    
    def get_all_mids(self) -> pd.DataFrame:
        """Get current mid prices for all assets"""
        try:
            payload = {"type": "allMids"}
            response = self._make_info_request(payload)
            
            if not response:
                return pd.DataFrame()
            
            # Convert to DataFrame
            data = []
            for asset, price in response.items():
                data.append({
                    'symbol': asset,
                    'mid_price': float(price),
                    'timestamp': datetime.now()
                })
            
            return pd.DataFrame(data)
            
        except Exception as e:
            self.logger.error(f"Failed to get mid prices: {e}")
            return pd.DataFrame()
    
    def get_market_data(self, symbol: str = 'ETH', interval: str = '1h', lookback_hours: int = 24) -> pd.DataFrame:
        """
        Get OHLCV candlestick data
        
        Args:
            symbol: Trading symbol (e.g., 'ETH', 'BTC')
            interval: Time interval ('1m', '5m', '15m', '1h', '4h', '1d')
            lookback_hours: Hours of historical data to fetch
        """
        try:
            # Calculate start time (in milliseconds)
            start_time = int((datetime.now() - timedelta(hours=lookback_hours)).timestamp() * 1000)
            
            payload = {
                "type": "candleSnapshot",
                "req": {
                    "coin": symbol,
                    "interval": interval,
                    "startTime": start_time
                }
            }
            
            response = self._make_info_request(payload)
            
            if not response:
                return pd.DataFrame()
            
            # Convert to DataFrame - response is a list of OHLCV data
            df = pd.DataFrame(response)
            
            if df.empty:
                return df
            
            # Hyperliquid returns: [timestamp, open, high, low, close, volume]
            if len(df.columns) >= 6:
                df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                
                # Convert timestamp from milliseconds
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                
                # Convert price/volume columns to numeric
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Set timestamp as index
                df = df.set_index('timestamp').sort_index()
                df['symbol'] = symbol
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to get market data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_funding_rates(self) -> pd.DataFrame:
        """Get funding rates for all perpetual futures"""
        try:
            payload = {"type": "metaAndAssetCtxs"}
            response = self._make_info_request(payload)
            
            if not response or len(response) < 2:
                return pd.DataFrame()
            
            # Extract funding rate data
            asset_contexts = response[1]  # Second element contains asset contexts
            
            funding_data = []
            for asset_ctx in asset_contexts:
                if 'funding' in asset_ctx:
                    funding_data.append({
                        'symbol': asset_ctx.get('name', 'unknown'),
                        'funding_rate': float(asset_ctx['funding']),
                        'timestamp': datetime.now()
                    })
            
            return pd.DataFrame(funding_data)
            
        except Exception as e:
            self.logger.error(f"Failed to get funding rates: {e}")
            return pd.DataFrame()
    
    def get_user_state(self, user_address: str) -> pd.DataFrame:
        """Get user's current positions and balances"""
        try:
            payload = {
                "type": "clearinghouseState",
                "user": user_address
            }
            
            response = self._make_info_request(payload)
            
            if not response:
                return pd.DataFrame()
            
            # Process balance and position data
            data = {
                'user_address': user_address,
                'timestamp': datetime.now(),
                'account_value': response.get('marginSummary', {}).get('accountValue', 0),
                'total_margin_used': response.get('marginSummary', {}).get('totalMarginUsed', 0)
            }
            
            return pd.DataFrame([data])
            
        except Exception as e:
            self.logger.error(f"Failed to get user state for {user_address}: {e}")
            return pd.DataFrame()
    
    def get_recent_trades(self, symbol: str = 'ETH') -> pd.DataFrame:
        """Get recent trades for a symbol"""
        try:
            payload = {
                "type": "recentTrades",
                "coin": symbol
            }
            
            response = self._make_info_request(payload)
            
            if not response:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(response)
            
            if df.empty:
                return df
            
            # Process trade data
            if 'time' in df.columns:
                df['timestamp'] = pd.to_datetime(df['time'], unit='ms')
            
            df['symbol'] = symbol
            
            # Convert numeric columns
            numeric_cols = ['px', 'sz']  # price, size
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to get recent trades for {symbol}: {e}")
            return pd.DataFrame()
    
    def validate_connection(self) -> bool:
        """Test if Hyperliquid API is accessible"""
        try:
            # Try to get mid prices
            df = self.get_all_mids()
            return not df.empty
        except:
            return False
    
    def get_available_symbols(self) -> List[str]:
        """Get list of available trading symbols"""
        try:
            payload = {"type": "meta"}
            response = self._make_info_request(payload)
            
            if response and 'universe' in response:
                return [asset['name'] for asset in response['universe']]
            
            return ['ETH', 'BTC']  # Fallback
            
        except:
            return ['ETH', 'BTC']  # Fallback
    
    def get_comprehensive_market_data(self, symbols: List[str] = None, hours_back: int = 24) -> pd.DataFrame:
        """
        Get comprehensive market data for analysis
        Similar to your Dune bot volume data but for DEX trading
        """
        if symbols is None:
            symbols = ['ETH', 'BTC', 'SOL', 'AVAX']  # Popular symbols
        
        all_data = []
        
        for symbol in symbols:
            try:
                # Get OHLCV data
                ohlcv = self.get_market_data(symbol, interval='1h', lookback_hours=hours_back)
                
                if not ohlcv.empty:
                    # Add symbol column
                    ohlcv['symbol'] = symbol
                    ohlcv = ohlcv.reset_index()
                    all_data.append(ohlcv)
                
                # Rate limiting
                import time
                time.sleep(0.1)
                
            except Exception as e:
                self.logger.error(f"Failed to get data for {symbol}: {e}")
                continue
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()
