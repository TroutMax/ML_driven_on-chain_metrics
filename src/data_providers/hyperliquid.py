"""
Hyperliquid API Client - Complete Implementation
Provides access to Hyperliquid DEX perpetual futures data, funding rates, and trading data
"""

from typing import Dict, Optional, List
import pandas as pd
import requests
import json
import os
import time
from datetime import datetime, timedelta
from .base import BaseDataProvider

class HyperliquidProvider(BaseDataProvider):
    """
    Hyperliquid DEX data provider
    Complete implementation for accessing perpetual futures data, funding rates, and trading data
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
    
    def validate_connection(self) -> bool:
        """Test if Hyperliquid API is accessible"""
        try:
            # Try to get mid prices as a connection test
            df = self.get_all_mids()
            return not df.empty
        except:
            return False
    
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
    
    def get_market_data(self, symbol: str, interval: str = '1h', lookback_hours: int = 24) -> pd.DataFrame:
        """
        Get OHLCV candlestick data
        
        Args:
            symbol: Trading symbol (e.g., 'ETH', 'BTC', 'HYPE')
            interval: Time interval ('1m', '5m', '15m', '1h', '4h', '1d')
            lookback_hours: Hours of historical data to fetch
        
        Returns:
            pandas.DataFrame: OHLCV data with datetime index
            
        API Response Format (corrected):
        - t: Start timestamp (Unix timestamp in milliseconds)
        - T: End timestamp (Unix timestamp in milliseconds)
        - s: Symbol (Trading pair or coin symbol)
        - i: Interval (Time interval of the candle)
        - o: Open price
        - c: Close price
        - h: High price
        - l: Low price
        - v: Volume
        - n: Number of trades
        """
        try:
            # Validate symbol parameter
            if not symbol:
                self.logger.error("Symbol parameter is required")
                return pd.DataFrame()
            
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
            
            self.logger.info(f"Requesting {symbol} {interval} data from {start_time}")
            response = self._make_info_request(payload)
            
            if not response:
                self.logger.warning(f"Empty response for {symbol}")
                return pd.DataFrame()
            
            # Response is a list of dictionaries
            if not isinstance(response, list):
                self.logger.error(f"Unexpected response format: {type(response)}")
                return pd.DataFrame()
            
            if len(response) == 0:
                self.logger.warning(f"No data in response for {symbol}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(response)
            
            if df.empty:
                return df
            
            self.logger.info(f"Received columns for {symbol}: {df.columns.tolist()}")
            self.logger.info(f"DataFrame shape: {df.shape}")
            
            # Map all columns - Hyperliquid returns: ['t', 'T', 's', 'i', 'o', 'c', 'h', 'l', 'v', 'n']
            # Fixed column mapping to handle all 10 columns correctly
            new_column_names = []
            for col in df.columns:
                if col == 't':
                    new_column_names.append('timestamp')
                elif col == 'T':
                    new_column_names.append('timestamp_end')
                elif col == 's':
                    new_column_names.append('symbol_api')
                elif col == 'i':
                    new_column_names.append('interval_api')
                elif col == 'o':
                    new_column_names.append('open')
                elif col == 'c':
                    new_column_names.append('close')
                elif col == 'h':
                    new_column_names.append('high')
                elif col == 'l':
                    new_column_names.append('low')
                elif col == 'v':
                    new_column_names.append('volume')
                elif col == 'n':
                    new_column_names.append('trades')
                else:
                    # Keep unknown columns as-is (future-proofing)
                    new_column_names.append(f'unknown_{col}')
            
            # Set new column names
            df.columns = new_column_names
            
            # Convert timestamp from milliseconds to datetime
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df = df.set_index('timestamp').sort_index()
            
            # Convert numeric columns
            for col in ['open', 'high', 'low', 'close', 'volume', 'trades']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Add symbol column for identification
            df['symbol'] = symbol
            
            # Keep only OHLCV columns for analysis
            keep_cols = ['open', 'high', 'low', 'close', 'volume', 'symbol']
            if 'trades' in df.columns:
                keep_cols.append('trades')
            
            existing_cols = [col for col in keep_cols if col in df.columns]
            df = df[existing_cols]
            
            self.logger.info(f"Successfully processed {len(df)} candles for {symbol}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to get market data for {symbol}: {e}")
            return pd.DataFrame()
    
    def collect_long_term_data(self, symbol: str, interval: str = '1d', days_back: int = 100) -> pd.DataFrame:
        """
        Collect long-term OHLCV data for a symbol
        
        Args:
            symbol: Trading symbol (e.g., 'HYPE', 'ETH', 'BTC')
            interval: Time interval ('1m', '5m', '15m', '1h', '4h', '1d')
            days_back: Number of days of historical data
        
        Returns:
            pandas.DataFrame: OHLCV data with metadata
        """
        self.logger.info(f"Collecting {days_back} days of {interval} data for {symbol}")
        
        try:
            # Convert days to hours
            hours_back = days_back * 24
            
            # Get the data using existing method
            df = self.get_market_data(
                symbol=symbol, 
                interval=interval, 
                lookback_hours=hours_back
            )
            
            if df.empty:
                self.logger.warning(f"No data received for {symbol}")
                return pd.DataFrame()
            
            # Add metadata
            df['collection_timestamp'] = datetime.now()
            df['data_source'] = 'hyperliquid'
            df['interval'] = interval
            
            # Log collection stats
            self.logger.info(f"Successfully collected {len(df)} candles for {symbol}")
            self.logger.info(f"Date range: {df.index.min()} to {df.index.max()}")
            self.logger.info(f"Price range: ${df['close'].min():.4f} - ${df['close'].max():.4f}")
            self.logger.info(f"Total volume: {df['volume'].sum():,.0f}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error collecting long-term data for {symbol}: {e}")
            return pd.DataFrame()
    
    def collect_multiple_symbols(self, symbols: List[str], interval: str = '1d', days_back: int = 100) -> Dict:
        """
        Collect data for multiple symbols
        
        Args:
            symbols: List of symbols to collect
            interval: Time interval for data
            days_back: Days of historical data
            
        Returns:
            Dict: Results for each symbol
        """
        self.logger.info(f"Collecting {interval} data for {len(symbols)} symbols ({days_back} days)")
        
        results = {}
        
        for i, symbol in enumerate(symbols):
            self.logger.info(f"Processing {symbol} ({i+1}/{len(symbols)})")
            
            # Get data
            df = self.collect_long_term_data(symbol, interval, days_back)
            
            if not df.empty:
                # Calculate statistics
                price_change = ((df['close'].iloc[-1] / df['close'].iloc[0]) - 1) * 100
                
                results[symbol] = {
                    'data': df,
                    'candles': len(df),
                    'price_change': price_change,
                    'volume_total': df['volume'].sum(),
                    'avg_daily_volume': df['volume'].mean(),
                    'volatility': df['close'].std(),
                    'date_range': (df.index.min(), df.index.max())
                }
                
                self.logger.info(f"✅ {symbol}: {len(df)} candles, {price_change:+.2f}% change")
            else:
                results[symbol] = None
                self.logger.warning(f"❌ {symbol}: Failed to collect data")
            
            # Rate limiting - be respectful to API
            if i < len(symbols) - 1:
                time.sleep(0.5)
        
        return results
    
    def collect_multi_timeframe_data(self, symbol: str, intervals: List[str] = None, days_back: int = 100) -> Dict:
        """
        Collect data for a single symbol across multiple timeframes
        
        Args:
            symbol: Trading symbol
            intervals: List of intervals to collect
            days_back: Days of historical data
            
        Returns:
            Dict: Data for each timeframe
        """
        if intervals is None:
            intervals = ['1h', '4h', '1d']
        
        self.logger.info(f"Collecting {symbol} data across {len(intervals)} timeframes")
        
        results = {}
        
        for interval in intervals:
            self.logger.info(f"Collecting {symbol} {interval} data")
            
            df = self.collect_long_term_data(symbol, interval, days_back)
            
            if not df.empty:
                results[interval] = {
                    'data': df,
                    'candles': len(df),
                    'date_range': (df.index.min(), df.index.max())
                }
                self.logger.info(f"✅ {symbol} {interval}: {len(df)} candles")
            else:
                results[interval] = None
                self.logger.warning(f"❌ {symbol} {interval}: Failed")
            
            # Rate limiting
            time.sleep(0.5)
        
        return results
    
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
    
    def get_recent_trades(self, symbol: str) -> pd.DataFrame:
        """Get recent trades for a symbol"""
        try:
            # Validate symbol parameter
            if not symbol:
                self.logger.error("Symbol parameter is required")
                return pd.DataFrame()
            
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
    
# Add these methods to the HyperliquidProvider class:

    def get_market_overview(self, limit: int = 20) -> pd.DataFrame:
        """Get market overview with key metrics for top tokens"""
        try:
            # Get all mids and symbols
            mids = self.get_all_mids()
            symbols = self.get_available_symbols()
            
            overview_data = []
            for symbol in symbols[:limit]:
                try:
                    # Get 24h data for volume and price change
                    data_24h = self.get_market_data(symbol, '1h', lookback_hours=24)
                    if not data_24h.empty:
                        current_price = mids.get(symbol, 0)
                        volume_24h = data_24h['volume'].sum()
                        price_change_24h = ((current_price - data_24h.iloc[0]['close']) / data_24h.iloc[0]['close']) * 100
                        
                        overview_data.append({
                            'symbol': symbol,
                            'price': current_price,
                            'volume_24h': volume_24h,
                            'price_change_24h': price_change_24h,
                            'high_24h': data_24h['high'].max(),
                            'low_24h': data_24h['low'].min()
                        })
                except Exception as e:
                    logger.warning(f"Failed to get overview for {symbol}: {e}")
                    continue
            
            return pd.DataFrame(overview_data)
        except Exception as e:
            logger.error(f"Error getting market overview: {e}")
            return pd.DataFrame()

    def calculate_volatility(self, symbol: str, period_hours: int = 168) -> float:
        """Calculate volatility (standard deviation of returns) for a symbol"""
        try:
            data = self.get_market_data(symbol, '1h', lookback_hours=period_hours)
            if data.empty:
                return 0.0
            
            # Calculate hourly returns
            data['returns'] = data['close'].pct_change()
            volatility = data['returns'].std() * (24 ** 0.5)  # Annualized daily volatility
            return float(volatility)
        except Exception as e:
            logger.error(f"Error calculating volatility for {symbol}: {e}")
            return 0.0

    def get_price_momentum(self, symbol: str, short_period: int = 24, long_period: int = 168) -> Dict:
        """Calculate price momentum indicators"""
        try:
            data = self.get_market_data(symbol, '1h', lookback_hours=long_period)
            if data.empty or len(data) < long_period:
                return {}
            
            current_price = data['close'].iloc[-1]
            short_avg = data['close'].tail(short_period).mean()
            long_avg = data['close'].tail(long_period).mean()
            
            return {
                'symbol': symbol,
                'current_price': current_price,
                'sma_24h': short_avg,
                'sma_7d': long_avg,
                'momentum_24h': ((current_price - short_avg) / short_avg) * 100,
                'momentum_7d': ((current_price - long_avg) / long_avg) * 100,
                'trend': 'bullish' if short_avg > long_avg else 'bearish'
            }
        except Exception as e:
            logger.error(f"Error calculating momentum for {symbol}: {e}")
            return {}

    def get_volume_analysis(self, symbol: str, period_hours: int = 168) -> Dict:
        """Analyze volume patterns for a symbol"""
        try:
            data = self.get_market_data(symbol, '1h', lookback_hours=period_hours)
            if data.empty:
                return {}
            
            volume_24h = data['volume'].tail(24).sum()
            volume_7d = data['volume'].sum()
            avg_volume = data['volume'].mean()
            volume_std = data['volume'].std()
            
            # Calculate volume spike (current vs average)
            current_volume = data['volume'].iloc[-1]
            volume_spike = (current_volume - avg_volume) / volume_std if volume_std > 0 else 0
            
            return {
                'symbol': symbol,
                'volume_24h': volume_24h,
                'volume_7d': volume_7d,
                'avg_hourly_volume': avg_volume,
                'volume_spike_score': volume_spike,
                'high_volume_hours': len(data[data['volume'] > (avg_volume + volume_std)])
            }
        except Exception as e:
            logger.error(f"Error analyzing volume for {symbol}: {e}")
            return {}

    def get_support_resistance(self, symbol: str, period_hours: int = 168) -> Dict:
        """Calculate basic support and resistance levels"""
        try:
            data = self.get_market_data(symbol, '1h', lookback_hours=period_hours)
            if data.empty:
                return {}
            
            # Simple support/resistance based on recent highs/lows
            resistance = data['high'].quantile(0.95)
            support = data['low'].quantile(0.05)
            current_price = data['close'].iloc[-1]
            
            # Distance to support/resistance
            resistance_distance = ((resistance - current_price) / current_price) * 100
            support_distance = ((current_price - support) / current_price) * 100
            
            return {
                'symbol': symbol,
                'support_level': support,
                'resistance_level': resistance,
                'current_price': current_price,
                'distance_to_resistance_%': resistance_distance,
                'distance_to_support_%': support_distance,
                'position': 'near_resistance' if resistance_distance < 5 else 'near_support' if support_distance < 5 else 'mid_range'
            }
        except Exception as e:
            logger.error(f"Error calculating support/resistance for {symbol}: {e}")
            return {}

    def get_correlation_matrix(self, symbols: List[str], period_hours: int = 168) -> pd.DataFrame:
        """Calculate correlation matrix between symbols"""
        try:
            price_data = {}
            
            for symbol in symbols:
                data = self.get_market_data(symbol, '1h', lookback_hours=period_hours)
                if not data.empty:
                    # Use hourly returns for correlation
                    returns = data['close'].pct_change().dropna()
                    price_data[symbol] = returns
            
            if not price_data:
                return pd.DataFrame()
            
            # Create DataFrame and calculate correlation
            df = pd.DataFrame(price_data)
            correlation_matrix = df.corr()
            
            return correlation_matrix
        except Exception as e:
            logger.error(f"Error calculating correlation matrix: {e}")
            return pd.DataFrame()

    def get_risk_metrics(self, symbol: str, period_hours: int = 168) -> Dict:
        """Calculate risk metrics for a symbol"""
        try:
            data = self.get_market_data(symbol, '1h', lookback_hours=period_hours)
            if data.empty:
                return {}
            
            # Calculate returns
            data['returns'] = data['close'].pct_change().dropna()
            returns = data['returns'].dropna()
            
            if returns.empty:
                return {}
            
            # Risk metrics
            volatility = returns.std() * (24 ** 0.5)  # Daily volatility
            downside_returns = returns[returns < 0]
            downside_volatility = downside_returns.std() * (24 ** 0.5) if not downside_returns.empty else 0
            max_drawdown = (data['close'] / data['close'].cummax() - 1).min()
            
            # Value at Risk (95% confidence)
            var_95 = returns.quantile(0.05)
            
            return {
                'symbol': symbol,
                'volatility_daily': volatility,
                'downside_volatility': downside_volatility,
                'max_drawdown_%': max_drawdown * 100,
                'var_95_%': var_95 * 100,
                'sharpe_approx': returns.mean() / volatility if volatility > 0 else 0,
                'total_return_%': ((data['close'].iloc[-1] / data['close'].iloc[0]) - 1) * 100
            }
        except Exception as e:
            logger.error(f"Error calculating risk metrics for {symbol}: {e}")
            return {}
    
    def get_comprehensive_market_data(self, symbols: List[str] = None, hours_back: int = 24) -> pd.DataFrame:
        """
        Get comprehensive market data for analysis
        Similar to your Dune bot volume data but for DEX trading
        """
        if symbols is None:
            symbols = ['ETH', 'BTC', 'SOL', 'AVAX', 'HYPE']  # Popular symbols
        
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
                time.sleep(0.1)
                
            except Exception as e:
                self.logger.error(f"Failed to get data for {symbol}: {e}")
                continue
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def save_data_to_file(self, df: pd.DataFrame, symbol: str, interval: str, data_dir: str = None) -> str:
        """
        Save market data to parquet file
        
        Args:
            df: DataFrame to save
            symbol: Trading symbol
            interval: Time interval
            data_dir: Directory to save data
            
        Returns:
            str: Path to saved file
        """
        try:
            if df.empty:
                self.logger.warning("No data to save")
                return None
            
            if data_dir is None:
                # Use a default data directory structure
                data_dir = os.path.join('data', 'raw', 'hyperliquid')
            
            os.makedirs(data_dir, exist_ok=True)
            
            # Create filename with metadata
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            date_range = f"{df.index.min().strftime('%Y%m%d')}_to_{df.index.max().strftime('%Y%m%d')}"
            filename = f"{symbol.lower()}_{interval}_{date_range}_{timestamp}.parquet"
            
            file_path = os.path.join(data_dir, filename)
            
            # Save data
            df.to_parquet(file_path)
            
            # Log file info
            file_size_mb = os.path.getsize(file_path) / 1024 / 1024
            self.logger.info(f"Data saved: {filename} ({file_size_mb:.2f} MB, {len(df)} records)")
            
            return file_path
            
        except Exception as e:
            self.logger.error(f"Failed to save data: {e}")
            return None
    
    def load_saved_data(self, symbol: str = None, interval: str = None, data_dir: str = None, latest_only: bool = True) -> pd.DataFrame:
        """
        Load previously saved market data
        
        Args:
            symbol: Specific symbol to load
            interval: Specific interval to load
            data_dir: Directory to search for data files
            latest_only: If True, load only the most recent file
            
        Returns:
            pandas.DataFrame: Loaded market data
        """
        try:
            import glob
            
            if data_dir is None:
                data_dir = os.path.join('data', 'raw', 'hyperliquid')
            
            # Build search pattern
            if symbol and interval:
                pattern = f"{data_dir}/{symbol.lower()}_{interval}_*.parquet"
            elif symbol:
                pattern = f"{data_dir}/{symbol.lower()}_*.parquet"
            else:
                pattern = f"{data_dir}/*.parquet"
            
            files = glob.glob(pattern)
            
            if not files:
                self.logger.warning(f"No files found for pattern: {pattern}")
                return pd.DataFrame()
            
            self.logger.info(f"Found {len(files)} files")
            
            if latest_only:
                # Get most recent file
                latest_file = max(files, key=os.path.getctime)
                self.logger.info(f"Loading latest: {os.path.basename(latest_file)}")
                
                df = pd.read_parquet(latest_file)
                self.logger.info(f"Loaded {len(df)} rows from {df.index.min()} to {df.index.max()}")
                return df
            else:
                # Load all files and concatenate
                all_data = []
                for file in files:
                    df = pd.read_parquet(file)
                    all_data.append(df)
                    self.logger.info(f"Loaded {os.path.basename(file)}: {len(df)} rows")
                
                if all_data:
                    combined_df = pd.concat(all_data, ignore_index=False)
                    # Remove duplicates and sort
                    combined_df = combined_df.drop_duplicates().sort_index()
                    return combined_df
                else:
                    return pd.DataFrame()
                    
        except Exception as e:
            self.logger.error(f"Failed to load data: {e}")
            return pd.DataFrame()