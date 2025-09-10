# Example Usage of the Data Provider System

# First install required packages if not already installed
# pip install dune-client requests pandas python-dotenv

# Load environment variables
from dotenv import load_dotenv
load_dotenv(override=True)

# Import our data provider system
from src.data_providers import setup_providers, DataProviderFactory

def main():
    """Main example demonstrating the provider system"""
    
    print("🚀 ML-Driven On-Chain Metrics - Data Provider Demo")
    print("=" * 60)
    
    # Method 1: Use the factory setup (recommended)
    print("\n📊 Setting up all providers...")
    manager = setup_providers()
    
    active_providers = manager.get_active_providers()
    print(f"✅ Active providers: {active_providers}")
    
    # Get health status
    health = manager.get_health_status()
    for provider_name, status in health.items():
        print(f"   {provider_name}: {status['status']} (requests: {status['requests_made']})")
    
    # Method 2: Create individual providers
    print("\n🔧 Creating individual providers...")
    
    # Dune provider example
    try:
        dune = DataProviderFactory.create_provider('dune')
        print("✅ Dune provider created")
        
        # Fetch your bot volume data
        bot_data = dune.get_bot_volume_data()
        print(f"📈 Bot volume data: {len(bot_data)} rows")
        print(f"   Columns: {list(bot_data.columns)}")
        
        # Show sample
        if len(bot_data) > 0:
            print(f"   Sample data shape: {bot_data.head(3).shape}")
            
    except Exception as e:
        print(f"❌ Dune provider error: {e}")
    
    # Hyperliquid provider example  
    try:
        hyperliquid = DataProviderFactory.create_provider('hyperliquid')
        print("✅ Hyperliquid provider created")
        
        # Fetch ETH market data
        eth_data = hyperliquid.get_market_data('ETH', '1h')
        print(f"📊 ETH market data: {len(eth_data)} rows")
        if len(eth_data) > 0:
            print(f"   Columns: {list(eth_data.columns)}")
            print(f"   Date range: {eth_data.index.min()} to {eth_data.index.max()}")
        
        # Get recent trades
        trades = hyperliquid.get_recent_trades('ETH')
        print(f"🔄 Recent ETH trades: {len(trades)} trades")
        
    except Exception as e:
        print(f"❌ Hyperliquid provider error: {e}")
    
    print("\n✨ Demo complete!")

if __name__ == "__main__":
    main()
