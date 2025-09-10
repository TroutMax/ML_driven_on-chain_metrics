"""
Data Provider Factory and Utils
"""

import os
from typing import Dict, Any, Optional, Type
from .base import BaseDataProvider
from .dune import DuneProvider
from .hyperliquid import HyperliquidProvider

class DataProviderFactory:
    """Factory for creating data provider instances"""
    
    _providers = {
        'dune': DuneProvider,
        'hyperliquid': HyperliquidProvider
    }
    
    @classmethod
    def create_provider(cls, provider_name: str, **kwargs) -> BaseDataProvider:
        """Create a data provider instance"""
        provider_name = provider_name.lower()
        
        if provider_name not in cls._providers:
            available = ', '.join(cls._providers.keys())
            raise ValueError(f"Unknown provider '{provider_name}'. Available: {available}")
        
        provider_class = cls._providers[provider_name]
        return provider_class(**kwargs)
    
    @classmethod
    def get_available_providers(cls) -> list:
        """Get list of available provider names"""
        return list(cls._providers.keys())
    
    @classmethod
    def register_provider(cls, name: str, provider_class: Type[BaseDataProvider]):
        """Register a new provider class"""
        cls._providers[name.lower()] = provider_class

class MultiProviderManager:
    """Manage multiple data providers"""
    
    def __init__(self):
        self.providers = {}
        self.active_providers = []
    
    def add_provider(self, name: str, provider: BaseDataProvider):
        """Add a provider instance"""
        self.providers[name] = provider
        if provider.validate_connection():
            self.active_providers.append(name)
            provider.logger.info(f"✅ {name.title()} provider connected successfully")
        else:
            provider.logger.warning(f"⚠️ {name.title()} provider connection failed")
    
    def get_provider(self, name: str) -> Optional[BaseDataProvider]:
        """Get a specific provider"""
        return self.providers.get(name)
    
    def get_active_providers(self) -> list:
        """Get list of active (connected) providers"""
        return self.active_providers.copy()
    
    def test_all_connections(self) -> Dict[str, bool]:
        """Test all provider connections"""
        results = {}
        for name, provider in self.providers.items():
            results[name] = provider.validate_connection()
        return results
    
    def get_health_status(self) -> Dict[str, Dict[str, Any]]:
        """Get health status of all providers"""
        status = {}
        for name, provider in self.providers.items():
            status[name] = provider.get_health_status()
        return status

def setup_providers() -> MultiProviderManager:
    """Setup and initialize all available providers"""
    manager = MultiProviderManager()
    
    # Setup Dune provider if API key available
    if os.getenv('DUNE_API_KEY'):
        try:
            dune = DataProviderFactory.create_provider('dune')
            manager.add_provider('dune', dune)
        except Exception as e:
            print(f"Failed to setup Dune provider: {e}")
    
    # Setup Hyperliquid provider (doesn't require API key for public data)
    try:
        hyperliquid = DataProviderFactory.create_provider('hyperliquid')
        manager.add_provider('hyperliquid', hyperliquid)
    except Exception as e:
        print(f"Failed to setup Hyperliquid provider: {e}")
    
    return manager

# Example usage function
def demo_usage():
    """Demo how to use the provider system"""
    print("🚀 Setting up data providers...")
    
    # Setup all providers
    manager = setup_providers()
    
    print(f"📊 Active providers: {manager.get_active_providers()}")
    
    # Test connections
    health = manager.test_all_connections()
    for provider, is_healthy in health.items():
        status = "✅ Connected" if is_healthy else "❌ Failed"
        print(f"   {provider.title()}: {status}")
    
    # Example data fetching
    dune = manager.get_provider('dune')
    if dune:
        print("\n📈 Fetching Dune data...")
        try:
            bot_data = dune.get_bot_volume_data()
            print(f"   Got {len(bot_data)} rows of bot volume data")
        except Exception as e:
            print(f"   Error: {e}")
    
    hyperliquid = manager.get_provider('hyperliquid')
    if hyperliquid:
        print("\n🔥 Fetching Hyperliquid data...")
        try:
            eth_data = hyperliquid.get_market_data('ETH', '1h')
            print(f"   Got {len(eth_data)} rows of ETH market data")
        except Exception as e:
            print(f"   Error: {e}")
    
    return manager
