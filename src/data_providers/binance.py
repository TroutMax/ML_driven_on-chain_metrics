"""
Binance API Client - Spot and Futures Historical Data
Provides access to Binance spot and perpetual futures historical price data
"""

import requests
import pandas as pd  
import time
from typing import Dict, Optional, List, Union
from datetime import datetime, timedelta
import logging
from .base import BaseDataProvider

class BinanceProvider(BaseDataProvider):
    """Standard method order"""
    
    def __init__(self, api_key: Optional[str] = None):
        # Binance doesn't require API key for public historical data
        super().__init__(api_key=api_key, rate_limit=10)  # 10 requests per second limit
        
        # Binance has separate endpoints for spot and futures
        self.spot_base_url = "https://api.binance.com/api/v3"
        self.futures_base_url = "https://fapi.binance.com/fapi/v1"
        
        # Cache for symbol info
        self._spot_symbols = None
        self._futures_symbols = None
        
    def _get_auth_headers(self) -> Dict[str, str]:
        """No auth headers needed for public data"""
        return {}
    
    def _get_base_url(self) -> str:
        """Return spot URL as default"""
        return self.spot_base_url
    
    def validate_connection(self) -> bool:
        """Test connection to both spot and futures APIs"""
        try:
            # Test spot API
            spot_response = requests.get(f"{self.spot_base_url}/ping", timeout=10)
            spot_ok = spot_response.status_code == 200
            
            # Test futures API  
            futures_response = requests.get(f"{self.futures_base_url}/ping", timeout=10)
            futures_ok = futures_response.status_code == 200
            
            return spot_ok and futures_ok
            
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False

    # STANDARD INTERFACE METHODS (to match other providers)
    def get_available_symbols(self) -> List[str]:
        """
        Standard method name to match other providers
        Returns combined spot and futures symbols
        """
        spot_symbols = self.get_spot_symbols()
        futures_symbols = self.get_futures_symbols()
        
        # Combine and deduplicate
        all_symbols = list(set(spot_symbols + futures_symbols))
        return sorted(all_symbols)

    def get_all_mids(self) -> pd.DataFrame:
        """
        Get current market prices (to match Hyperliquid interface)
        Returns 24hr ticker data for all symbols
        """
        try:
            # Get 24hr ticker statistics from spot market
            response = requests.get(f"{self.spot_base_url}/ticker/24hr", timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Standardize column names
            df = df.rename(columns={
                'symbol': 'symbol',
                'lastPrice': 'price',
                'volume': 'volume',
                'priceChangePercent': 'change_24h'
            })
            
            # Select relevant columns
            cols = ['symbol', 'price', 'volume', 'change_24h']
            df = df[cols]
            
            # Convert price to numeric
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
            df['change_24h'] = pd.to_numeric(df['change_24h'], errors='coerce')
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to get market prices: {e}")
            return pd.DataFrame()

    def get_market_data(self, symbol: str, **kwargs) -> pd.DataFrame:
        """
        Standard market data method to match other providers
        This should be the main entry point for getting market data
        """
        # Extract common parameters
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date') 
        market_type = kwargs.get('market_type', 'spot')
        limit = kwargs.get('limit', 1000)
        
        if start_date:
            # Use long-term data collection for date ranges
            return self.get_long_term_data(
                symbol=symbol,
                interval=interval,
                start_date=start_date,
                end_date=end_date,
                market_type=market_type
            )
        else:
            # Use direct klines for recent data
            return self.get_historical_klines(
                symbol=symbol,
                interval=interval,
                limit=limit,
                market_type=market_type
            )

    # BINANCE-SPECIFIC METHODS
    def get_spot_symbols(self, refresh: bool = False) -> List[str]:
        """
        Get all available spot trading symbols
        
        Args:
            refresh: Force refresh of cached symbols
            
        Returns:
            List of spot symbols (e.g., ['BTCUSDT', 'ETHUSDT', ...])
        """
        if self._spot_symbols is None or refresh:
            try:
                response = requests.get(f"{self.spot_base_url}/exchangeInfo", timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Extract active trading symbols
                symbols = []
                for symbol_info in data.get('symbols', []):
                    if symbol_info.get('status') == 'TRADING':
                        symbols.append(symbol_info['symbol'])
                
                self._spot_symbols = sorted(symbols)
                self.logger.info(f"Retrieved {len(self._spot_symbols)} spot symbols")
                
            except Exception as e:
                self.logger.error(f"Failed to get spot symbols: {e}")
                return []
        
        return self._spot_symbols

    def get_futures_symbols(self, refresh: bool = False) -> List[str]:
        """
        Get all available futures trading symbols
        
        Returns:
            List of futures symbols (e.g., ['BTCUSDT', 'ETHUSDT', ...])
        """
        if self._futures_symbols is None or refresh:
            try:
                response = requests.get(f"{self.futures_base_url}/exchangeInfo", timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Extract active trading symbols
                symbols = []
                for symbol_info in data.get('symbols', []):
                    if symbol_info.get('status') == 'TRADING':
                        symbols.append(symbol_info['symbol'])
                
                self._futures_symbols = sorted(symbols)
                self.logger.info(f"Retrieved {len(self._futures_symbols)} futures symbols")
                
            except Exception as e:
                self.logger.error(f"Failed to get futures symbols: {e}")
                return []
        
        return self._futures_symbols

    def search_symbols(self, search_term: str, market_type: str = 'both') -> Dict[str, List[str]]:
        """
        Search for symbols containing a specific term
        
        Args:
            search_term: Term to search for (e.g., 'BTC', 'ETH')
            market_type: 'spot', 'futures', or 'both'
            
        Returns:
            Dict with 'spot' and/or 'futures' lists
        """
        results = {}
        search_upper = search_term.upper()
        
        if market_type in ['spot', 'both']:
            spot_symbols = self.get_spot_symbols()
            results['spot'] = [s for s in spot_symbols if search_upper in s]
        
        if market_type in ['futures', 'both']:
            futures_symbols = self.get_futures_symbols()  
            results['futures'] = [s for s in futures_symbols if search_upper in s]
        
        return results

    def get_historical_klines(self, 
                            symbol: str, 
                            interval: str = '1d',
                            start_time: Optional[Union[str, datetime]] = None,
                            end_time: Optional[Union[str, datetime]] = None,
                            limit: int = 1000,
                            market_type: str = 'spot') -> pd.DataFrame:
        """
        Get historical kline/candlestick data
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            interval: Time interval ('1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M')
            start_time: Start time (str like '2020-01-01' or datetime)
            end_time: End time (str like '2024-01-01' or datetime)  
            limit: Max number of records (max 1000 per request)
            market_type: 'spot' or 'futures'
            
        Returns:
            DataFrame with OHLCV data
        """
        
        # Choose the right API endpoint
        if market_type == 'spot':
            base_url = self.spot_base_url
            endpoint = '/klines'
        elif market_type == 'futures':
            base_url = self.futures_base_url  
            endpoint = '/klines'
        else:
            raise ValueError("market_type must be 'spot' or 'futures'")
        
        # Build request parameters
        params = {
            'symbol': symbol.upper(),
            'interval': interval,
            'limit': min(limit, 1000)  # Binance max is 1000
        }
        
        # Add time parameters if provided
        if start_time:
            if isinstance(start_time, str):
                start_time = datetime.strptime(start_time, '%Y-%m-%d')
            params['startTime'] = int(start_time.timestamp() * 1000)
        
        if end_time:
            if isinstance(end_time, str):
                end_time = datetime.strptime(end_time, '%Y-%m-%d')
            params['endTime'] = int(end_time.timestamp() * 1000)
        
        try:
            self.logger.info(f"Fetching {market_type} data for {symbol} ({interval})")
            
            response = requests.get(f"{base_url}{endpoint}", params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                self.logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades_count', 
                'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
            ])
            
            # Convert timestamps and prices
            df['datetime'] = pd.to_datetime(df['open_time'], unit='ms')
            df['close_datetime'] = pd.to_datetime(df['close_time'], unit='ms')
            
            # Convert price columns to float
            price_cols = ['open', 'high', 'low', 'close', 'volume', 'quote_volume']
            for col in price_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Add metadata
            df['symbol'] = symbol
            df['interval'] = interval
            df['market_type'] = market_type
            
            # Select and reorder columns
            final_cols = [
                'datetime', 'open', 'high', 'low', 'close', 'volume', 
                'symbol', 'interval', 'market_type', 'trades_count'
            ]
            
            df = df[final_cols].sort_values('datetime').reset_index(drop=True)
            
            self.logger.info(f"Retrieved {len(df)} candles for {symbol}")
            return df
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error processing {symbol} data: {e}")
            return pd.DataFrame()
        
            # Add these methods for long-term data collection

    def get_long_term_data(self, 
                        symbol: str, 
                        interval: str = '1d',
                        start_date: str = '2020-01-01',
                        end_date: Optional[str] = None,
                        market_type: str = 'spot') -> pd.DataFrame:
        """
        Get long-term historical data by making multiple API calls
        
        Args:
            symbol: Trading symbol
            interval: Time interval  
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format (default: today)
            market_type: 'spot' or 'futures'
            
        Returns:
            Complete historical DataFrame
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Calculate how many days per request (based on interval and 1000 limit)
        interval_minutes = self._get_interval_minutes(interval)
        max_days_per_request = (1000 * interval_minutes) // (24 * 60)
        
        all_data = []
        current_start = start_dt
        
        while current_start < end_dt:
            # Calculate end time for this chunk
            current_end = min(current_start + timedelta(days=max_days_per_request), end_dt)
            
            # Get data for this chunk
            chunk_data = self.get_historical_klines(
                symbol=symbol,
                interval=interval,
                start_time=current_start,
                end_time=current_end,
                limit=1000,
                market_type=market_type
            )
            
            if not chunk_data.empty:
                all_data.append(chunk_data)
                self.logger.info(f"Collected {len(chunk_data)} records from {current_start.date()} to {current_end.date()}")
            
            # Move to next chunk
            current_start = current_end + timedelta(days=1)
            
            # Rate limiting - be nice to Binance
            time.sleep(0.1)
        
        if all_data:
            # Combine all chunks
            complete_df = pd.concat(all_data, ignore_index=True)
            # Remove any duplicates
            complete_df = complete_df.drop_duplicates(subset=['datetime', 'symbol']).reset_index(drop=True)
            
            self.logger.info(f"Collected complete dataset: {len(complete_df)} records from {start_date} to {end_date}")
            return complete_df
        else:
            return pd.DataFrame()

    def _get_interval_minutes(self, interval: str) -> int:
        """Convert interval string to minutes"""
        interval_map = {
            '1m': 1, '3m': 3, '5m': 5, '15m': 15, '30m': 30,
            '1h': 60, '2h': 120, '4h': 240, '6h': 360, '8h': 480, '12h': 720,
            '1d': 1440, '3d': 4320, '1w': 10080, '1M': 43800
        }
        return interval_map.get(interval, 1440)  # Default to daily
    
    def collect_multiple_symbols(self, 
                            symbols: List[str],
                            interval: str = '1d', 
                            start_date: str = '2020-01-01',
                            end_date: Optional[str] = None,
                            market_type: str = 'spot') -> Dict[str, pd.DataFrame]:
        """
        Collect historical data for multiple symbols
        
        Returns:
            Dict mapping symbol to DataFrame
        """
        results = {}
        
        for i, symbol in enumerate(symbols):
            try:
                self.logger.info(f"Collecting {symbol} ({i+1}/{len(symbols)})")
                
                df = self.get_long_term_data(
                    symbol=symbol,
                    interval=interval, 
                    start_date=start_date,
                    end_date=end_date,
                    market_type=market_type
                )
                
                if not df.empty:
                    results[symbol] = df
                    self.logger.info(f"✅ {symbol}: {len(df)} records")
                else:
                    results[symbol] = pd.DataFrame()
                    self.logger.warning(f"❌ {symbol}: No data")
                
                # Rate limiting between symbols
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Failed to collect {symbol}: {e}")
                results[symbol] = pd.DataFrame()
        
        return results