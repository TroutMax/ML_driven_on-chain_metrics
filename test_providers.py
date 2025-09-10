"""Test the data providers setup"""

from src.data_providers import setup_providers, DataProviderFactory

def test_providers():
    """Test all providers"""
    print("🧪 Testing Data Providers Setup\n")
    
    # Test factory
    print("📋 Available providers:", DataProviderFactory.get_available_providers())
    
    # Setup all providers
    manager = setup_providers()
    
    # Test connections
    print("\n🔍 Testing connections...")
    connection_status = manager.test_all_connections()
    for provider, status in connection_status.items():
        status_emoji = "✅" if status else "❌"
        print(f"{status_emoji} {provider}: {'Connected' if status else 'Failed'}")
    
    # Get health status
    print("\n📊 Health Status:")
    health = manager.get_health_status()
    for provider, status in health.items():
        print(f"   {provider}: {status}")
    
    # Test Dune data if available
    dune = manager.get_provider('dune')
    if dune:
        print("\n📈 Testing Dune data...")
        try:
            bot_data = dune.get_bot_volume_data()
            print(f"   Bot volume data shape: {bot_data.shape}")
            print(f"   Columns: {bot_data.columns.tolist()}")
        except Exception as e:
            print(f"   Error: {e}")
    
    # Test Hyperliquid data
    hyperliquid = manager.get_provider('hyperliquid')
    if hyperliquid:
        print("\n🔥 Testing Hyperliquid data...")
        try:
            eth_data = hyperliquid.get_market_data('ETH', '1h')
            print(f"   ETH market data shape: {eth_data.shape}")
            if not eth_data.empty:
                print(f"   Latest price: ${eth_data['close'].iloc[-1]:,.2f}")
        except Exception as e:
            print(f"   Error: {e}")

if __name__ == "__main__":
    test_providers()