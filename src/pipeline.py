"""
Data Pipeline - Automated Data Collection and Processing
"""

import os
import time
import schedule
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional

from .data_providers import setup_providers, MultiProviderManager

class DataPipeline:
    """Automated data collection and processing pipeline"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        (self.data_dir / "raw").mkdir(exist_ok=True)
        (self.data_dir / "processed").mkdir(exist_ok=True)
        (self.data_dir / "cache").mkdir(exist_ok=True)
        
        self.manager = setup_providers()
        self.collection_log = []
        
    def collect_dune_data(self) -> Dict[str, pd.DataFrame]:
        """Collect data from Dune Analytics"""
        dune = self.manager.get_provider('dune')
        if not dune:
            return {}
        
        datasets = {}
        try:
            # Your bot volume data
            bot_data = dune.get_bot_volume_data()
            if len(bot_data) > 0:
                datasets['bot_volume'] = bot_data
                
                # Save to file
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = self.data_dir / "raw" / f"dune_bot_volume_{timestamp}.parquet"
                bot_data.to_parquet(filename)
                
                self.collection_log.append({
                    'timestamp': datetime.now(),
                    'provider': 'dune',
                    'dataset': 'bot_volume',
                    'rows': len(bot_data),
                    'file': str(filename)
                })
                
        except Exception as e:
            print(f"Error collecting Dune data: {e}")
            
        return datasets
    
    def collect_hyperliquid_data(self, symbols: List[str] = ['ETH', 'BTC']) -> Dict[str, pd.DataFrame]:
        """Collect data from Hyperliquid"""
        hyperliquid = self.manager.get_provider('hyperliquid')
        if not hyperliquid:
            return {}
        
        datasets = {}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for symbol in symbols:
            try:
                # Market data
                market_data = hyperliquid.get_market_data(symbol, '1h')
                if len(market_data) > 0:
                    datasets[f'{symbol}_ohlcv'] = market_data
                    
                    filename = self.data_dir / "raw" / f"hyperliquid_{symbol}_ohlcv_{timestamp}.parquet"
                    market_data.to_parquet(filename)
                    
                    self.collection_log.append({
                        'timestamp': datetime.now(),
                        'provider': 'hyperliquid',
                        'dataset': f'{symbol}_ohlcv',
                        'rows': len(market_data),
                        'file': str(filename)
                    })
                
                # Funding rates
                funding = hyperliquid.get_funding_rates(symbol)
                if len(funding) > 0:
                    datasets[f'{symbol}_funding'] = funding
                    
                    filename = self.data_dir / "raw" / f"hyperliquid_{symbol}_funding_{timestamp}.parquet"
                    funding.to_parquet(filename)
                    
                    self.collection_log.append({
                        'timestamp': datetime.now(),
                        'provider': 'hyperliquid',
                        'dataset': f'{symbol}_funding',
                        'rows': len(funding),
                        'file': str(filename)
                    })
                
                # Recent trades
                trades = hyperliquid.get_recent_trades(symbol)
                if len(trades) > 0:
                    datasets[f'{symbol}_trades'] = trades
                    
                    filename = self.data_dir / "raw" / f"hyperliquid_{symbol}_trades_{timestamp}.parquet"
                    trades.to_parquet(filename)
                    
                # Sleep between symbols to respect rate limits
                time.sleep(1)
                
            except Exception as e:
                print(f"Error collecting {symbol} data from Hyperliquid: {e}")
                
        return datasets
    
    def run_full_collection(self) -> Dict[str, Any]:
        """Run complete data collection cycle"""
        print(f"🚀 Starting data collection at {datetime.now()}")
        
        start_time = time.time()
        all_datasets = {}
        
        # Collect from all providers
        dune_data = self.collect_dune_data()
        hyperliquid_data = self.collect_hyperliquid_data()
        
        all_datasets.update(dune_data)
        all_datasets.update(hyperliquid_data)
        
        # Collection summary
        total_rows = sum(len(df) for df in all_datasets.values())
        elapsed = time.time() - start_time
        
        summary = {
            'timestamp': datetime.now(),
            'datasets_collected': len(all_datasets),
            'total_rows': total_rows,
            'elapsed_seconds': elapsed,
            'datasets': list(all_datasets.keys())
        }
        
        print(f"✅ Collection complete: {len(all_datasets)} datasets, {total_rows} total rows")
        print(f"   Time elapsed: {elapsed:.1f} seconds")
        
        # Save collection log
        log_df = pd.DataFrame(self.collection_log)
        if len(log_df) > 0:
            log_file = self.data_dir / "collection_log.parquet"
            log_df.to_parquet(log_file)
        
        return summary
    
    def create_consolidated_dataset(self) -> pd.DataFrame:
        """Create a consolidated dataset for ML training"""
        print("📊 Creating consolidated dataset...")
        
        # Load latest data files
        raw_dir = self.data_dir / "raw"
        latest_files = {}
        
        # Find latest files for each dataset type
        for file in raw_dir.glob("*.parquet"):
            parts = file.stem.split('_')
            if len(parts) >= 3:
                provider = parts[0]
                dataset_type = '_'.join(parts[1:-1])  # everything except timestamp
                key = f"{provider}_{dataset_type}"
                
                if key not in latest_files or file.stat().st_mtime > latest_files[key].stat().st_mtime:
                    latest_files[key] = file
        
        # Load and combine datasets
        combined_data = {}
        for key, file in latest_files.items():
            try:
                df = pd.read_parquet(file)
                combined_data[key] = df
                print(f"   Loaded {key}: {len(df)} rows")
            except Exception as e:
                print(f"   Error loading {file}: {e}")
        
        # Basic feature engineering could go here
        # For now, just save the latest datasets
        processed_file = self.data_dir / "processed" / f"consolidated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        
        # Create a simple summary dataset
        summary_data = []
        for key, df in combined_data.items():
            summary_data.append({
                'dataset': key,
                'rows': len(df),
                'columns': len(df.columns),
                'latest_timestamp': datetime.now()
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_parquet(processed_file)
        
        print(f"📁 Consolidated dataset saved: {processed_file}")
        return summary_df
    
    def schedule_collection(self, interval_minutes: int = 60):
        """Schedule automatic data collection"""
        print(f"⏰ Scheduling data collection every {interval_minutes} minutes")
        
        schedule.every(interval_minutes).minutes.do(self.run_full_collection)
        
        # Also schedule daily consolidation
        schedule.every().day.at("02:00").do(self.create_consolidated_dataset)
        
        print("🔄 Starting scheduled collection... (Press Ctrl+C to stop)")
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            print("\n⏹️ Scheduled collection stopped")
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get statistics about data collection"""
        log_file = self.data_dir / "collection_log.parquet"
        
        if not log_file.exists():
            return {"message": "No collection log found"}
        
        log_df = pd.read_parquet(log_file)
        
        stats = {
            'total_collections': len(log_df),
            'providers': log_df['provider'].value_counts().to_dict(),
            'datasets': log_df['dataset'].value_counts().to_dict(),
            'total_rows_collected': log_df['rows'].sum(),
            'latest_collection': log_df['timestamp'].max(),
            'collection_frequency': log_df.groupby('provider')['timestamp'].nunique().to_dict()
        }
        
        return stats

# Convenience function
def run_data_collection():
    """Quick function to run data collection once"""
    pipeline = DataPipeline()
    return pipeline.run_full_collection()

# Convenience function for scheduling
def start_automated_collection(interval_minutes: int = 60):
    """Start automated data collection"""
    pipeline = DataPipeline()
    pipeline.schedule_collection(interval_minutes)
