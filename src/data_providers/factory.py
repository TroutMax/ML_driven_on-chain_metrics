"""
Data Provider Factory and Utils
"""

import os
from typing import Dict, List, Optional, Type
from .base import BaseDataProvider
from .dune import DuneProvider
from .hyperliquid import HyperliquidProvider

class DataProviderFactory:
    """Factory for creating data provider instances"""
    
    _providers: Dict[str, Type[BaseDataProvider]] = {
        'dune': DuneProvider,
        'hyperliquid': HyperliquidProvider,
    }

    @classmethod
    def create_provider(cls, provider_name: str, **kwargs) -> BaseDataProvider:
        """
        Create a data provider instance
        
        Args:
            provider_name: Name of the provider ('dune', 'hyperliquid', etc.)
            **kwargs: Additional arguments for provider initialization
        """
        if provider_name not in cls._providers:
            raise ValueError(f"Unknown provider: {provider_name}. Available: {list(cls._providers.keys())}")
        
        provider_class = cls._providers[provider_name]
        return provider_class(**kwargs)
    
    @classmethod
    def register_provider(cls, name: str, provider_class: Type[BaseDataProvider]):
        """Register a new provider class"""
        cls._providers[name] = provider_class
    
    @classmethod
    def get_available_providers(cls) -> List[str]:
        """Get list of available provider names"""
        return list(cls._providers.keys())

class MultiProviderManager:
    """Manage multiple data providers"""
    
    def __init__(self):
        self.providers: Dict[str, BaseDataProvider] = {}
    
    def add_provider(self, name: str, provider: BaseDataProvider):
        """Add a provider to the manager"""
        self.providers[name] = provider
    
    def get_provider(self, name: str) -> Optional[BaseDataProvider]:
        """Get a specific provider"""
        return self.providers.get(name)
    
    def remove_provider(self, name: str):
        """Remove a provider"""
        if name in self.providers:
            del self.providers[name]
    
    def get_active_providers(self) -> List[str]:
        """Get list of active provider names"""
        return list(self.providers.keys())
    
    def test_all_connections(self) -> Dict[str, bool]:
        """Test connections for all providers"""
        results = {}
        for name, provider in self.providers.items():
            try:
                results[name] = provider.validate_connection()
            except Exception as e:
                print(f"Connection test failed for {name}: {e}")
                results[name] = False
        return results
    
    def get_health_status(self) -> Dict[str, Dict]:
        """Get health status for all providers"""
        status = {}
        for name, provider in self.providers.items():
            try:
                status[name] = provider.get_health_status()
            except Exception as e:
                status[name] = {'error': str(e)}
        return status

def setup_providers() -> MultiProviderManager:
    """
    Automatically setup all available providers
    Uses environment variables for authentication
    """
    manager = MultiProviderManager()
    
    # Setup Dune if API key is available
    if os.getenv('DUNE_API_KEY'):
        try:
            dune = DataProviderFactory.create_provider('dune')
            manager.add_provider('dune', dune)
            print("✅ Dune provider initialized")
        except Exception as e:
            print(f"❌ Failed to initialize Dune provider: {e}")
    else:
        print("⚠️  DUNE_API_KEY not found in environment")
    
    # Setup Hyperliquid (no API key needed for public data)
    try:
        hyperliquid = DataProviderFactory.create_provider('hyperliquid')
        manager.add_provider('hyperliquid', hyperliquid)
        print("✅ Hyperliquid provider initialized")
    except Exception as e:
        print(f"❌ Failed to initialize Hyperliquid provider: {e}")
    
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
