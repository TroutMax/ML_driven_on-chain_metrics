"""
Data Providers Package
Centralized API clients for all data sources
"""

from .base import BaseDataProvider
from .dune import DuneProvider
from .hyperliquid import HyperliquidProvider
from .factory import DataProviderFactory, MultiProviderManager, setup_providers

__all__ = [
    'BaseDataProvider',
    'DuneProvider', 
    'HyperliquidProvider',
    'DataProviderFactory',
    'MultiProviderManager',
    'setup_providers'
]


def setup_providers():
    """Setup and return configured data providers"""
    manager = DataProviderManager()
    
    # Dune provider (existing)
    dune_key = os.getenv('DUNE_API_KEY')
    if dune_key:
        manager.register_provider('dune', DuneProvider(api_key=dune_key))
    
    # Hyperliquid provider (new)
    # No API key needed for public data
    manager.register_provider('hyperliquid', HyperliquidProvider())
    
    return manager
