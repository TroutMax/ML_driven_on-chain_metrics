# ML-Driven On-Chain Metrics

A comprehensive machine learning framework for analyzing cryptocurrency trading bot performance and on-chain metrics using multiple data providers.

## 🏗️ Project Structure

```
ML_driven_on-chain_metrics/
├── .env                     # API keys (secure storage)
├── .gitignore              # Protects sensitive data
├── README.md               # This file
├── requirements.txt        # Python dependencies
├── example_usage.py        # Quick start examples
├── ideas.txt              # Project roadmap and ML strategies
├── notebooks/
│   └── Initial_EDA.ipynb   # Main analysis notebook
├── src/
│   ├── __init__.py
│   ├── pipeline.py         # Automated data collection
│   └── data_providers/     # Multi-provider API clients
│       ├── __init__.py
│       ├── base.py         # Abstract base class
│       ├── dune.py         # Dune Analytics client
│       ├── hyperliquid.py  # Hyperliquid DEX client
│       └── factory.py      # Provider factory and manager
└── data/                   # Local data storage (auto-created)
    ├── raw/               # Raw API data
    ├── processed/         # Processed datasets
    └── cache/             # Cached responses
```

## 🚀 Quick Start

### 1. Environment Setup

```bash
# Clone the repository
git clone <your-repo-url>
cd ML_driven_on-chain_metrics

# Install dependencies
pip install dune-client requests pandas python-dotenv plotly scikit-learn
# Optional: pip install xgboost lightgbm ta-lib schedule

# Set up your API keys
echo "DUNE_API_KEY=your_dune_api_key_here" > .env
```

### 2. Basic Usage

```python
from src.data_providers import setup_providers

# Setup all providers
manager = setup_providers()
print(f"Active providers: {manager.get_active_providers()}")

# Get Dune data
dune = manager.get_provider('dune')
bot_data = dune.get_bot_volume_data()

# Get Hyperliquid data
hyperliquid = manager.get_provider('hyperliquid')
eth_data = hyperliquid.get_market_data('ETH', '1h')
```

### 3. Run the Example

```python
python example_usage.py
```

## 📊 Data Providers

### Dune Analytics (`DuneProvider`)
- **Purpose**: On-chain analytics and custom SQL queries
- **Features**: Query caching, rate limiting, trading bot metrics
- **Auth**: Requires `DUNE_API_KEY` in `.env`

**Key Methods:**
```python
dune.get_query_result(query_id)     # Execute any Dune query
dune.get_bot_volume_data()          # Your specific bot data
dune.clear_cache()                  # Clear query cache
```

### Hyperliquid (`HyperliquidProvider`)
- **Purpose**: DEX trading data and market information
- **Features**: OHLCV data, funding rates, order book, user trading history
- **Auth**: Public endpoints (no API key required for market data)

**Key Methods:**
```python
hyperliquid.get_market_data('ETH', '1h')     # OHLCV candlestick data
hyperliquid.get_funding_rates('ETH')         # Funding rate history
hyperliquid.get_recent_trades('ETH')         # Recent trade data
hyperliquid.get_user_fills(user_address)     # User trading history
```

### Adding New Providers

1. **Create provider class** inheriting from `BaseDataProvider`
2. **Implement required methods**: `_get_auth_headers()`, `get_market_data()`, `validate_connection()`
3. **Register with factory**: `DataProviderFactory.register_provider('name', YourProvider)`

## 🔄 Automated Data Pipeline

### One-time Collection
```python
from src.pipeline import run_data_collection
summary = run_data_collection()
```

### Scheduled Collection
```python
from src.pipeline import start_automated_collection
start_automated_collection(interval_minutes=60)  # Collect every hour
```

### Pipeline Features
- **Automatic data collection** from all active providers
- **Local storage** in Parquet format for efficiency
- **Data consolidation** and basic preprocessing
- **Collection logging** and statistics
- **Rate limit compliance** across all providers

## 🧠 Machine Learning Integration

### Current ML Examples (in `Initial_EDA.ipynb`)
- **Feature Engineering**: Price movements, volume ratios, technical indicators
- **Classification**: Profitable vs unprofitable trading periods
- **Visualization**: Interactive Plotly charts and dashboards

### Recommended ML Stack
```python
# Core ML
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# Advanced ML (optional)
import xgboost as xgb
import lightgbm as lgb

# Technical Analysis
import ta

# Time Series
from statsmodels.tsa.arima.model import ARIMA
```
# Data Directory Structure

## Raw Data
- `raw/dune/`: Raw blockchain data from Dune Analytics
- `raw/hyperliquid/`: Raw DEX data from Hyperliquid
- `raw/backup/`: Critical dataset backups

## Processed Data  
- `processed/daily/`: Daily aggregated metrics
- `processed/hourly/`: Hourly features for real-time analysis
- `processed/features/`: ML-ready feature datasets

## Cache & Temporary
- `cache/`: Temporary processing files
- `cache/api_responses/`: Cached API calls (1-hour TTL)

## Models & Outputs
- `models/`: Trained ML models and scalers
- `exports/`: Clean datasets for sharing
- `metadata/`: Data schemas and quality reports

## File Naming Convention
- Raw: `{source}_{dataset}_{YYYYMMDD_HHMMSS}.parquet`
- Processed: `{feature_type}_{timeframe}_{YYYYMMDD}.parquet`
- Models: `{model_type}_{version}_{YYYYMMDD}.pkl`

### ML Signal Ideas (see `ideas.txt`)
1. **Volume Anomaly Detection** - Identify unusual trading patterns
2. **Cross-Exchange Arbitrage** - Price difference signals
3. **Funding Rate Momentum** - Perpetual futures funding trends
4. **Whale Movement Detection** - Large transaction analysis
5. **Market Sentiment Analysis** - Social + on-chain signals

## 🔧 Advanced Features

### Rate Limiting & Error Handling
- **Automatic rate limiting** with exponential backoff
- **Connection health monitoring** and automatic retries
- **Comprehensive logging** for debugging and monitoring

### Data Management
- **Efficient storage** using Parquet format
- **Automatic caching** to reduce API calls
- **Data versioning** with timestamps
- **Memory optimization** for large datasets

### Security Best Practices
- **Environment variables** for API keys
- **Comprehensive .gitignore** prevents key exposure
- **No hardcoded credentials** in any code files

## 📈 Visualization Examples

### Multi-Panel Dashboard
```python
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Create sophisticated trading dashboards
fig = make_subplots(
    rows=3, cols=1,
    subplot_titles=['Volume Analysis', 'Price Action', 'Bot Performance'],
    shared_xaxes=True
)
```

### Interactive Charts
- **Stacked area charts** for volume composition
- **Candlestick charts** for price analysis  
- **Scatter plots** for correlation analysis
- **Heatmaps** for performance metrics

## 🛠️ Development Workflow

### 1. Data Exploration
Use `notebooks/Initial_EDA.ipynb` for:
- Initial data analysis
- Feature engineering experiments
- Model prototyping
- Visualization development

### 2. Production Code
Move stable code to `src/` modules:
- New data providers → `src/data_providers/`
- ML models → `src/models/` (create as needed)
- Utilities → `src/utils/` (create as needed)

### 3. Testing & Validation
```python
# Test provider connections
manager = setup_providers()
health = manager.test_all_connections()

# Validate data quality
pipeline = DataPipeline()
stats = pipeline.get_collection_stats()
```

## 📋 Requirements

### Core Dependencies
```
dune-client>=1.2.0
requests>=2.28.0
pandas>=1.5.0
python-dotenv>=0.19.0
plotly>=5.11.0
scikit-learn>=1.1.0
```

### Optional (for advanced features)
```
xgboost>=1.6.0          # Gradient boosting
lightgbm>=3.3.0         # Fast gradient boosting
ta>=0.10.0              # Technical analysis
schedule>=1.2.0         # Job scheduling
statsmodels>=0.13.0     # Time series analysis
```

## 🤝 Contributing

1. **Add new data providers** following the `BaseDataProvider` pattern
2. **Enhance ML models** with new features and algorithms
3. **Improve data pipeline** with better processing and storage
4. **Add comprehensive tests** for reliability

## 📝 Next Steps

1. **Complete provider implementations** for other exchanges (Binance, Coinbase, etc.)
2. **Advanced ML pipelines** with automated model training and evaluation  
3. **Real-time data streaming** for live trading signals
4. **Web dashboard** for monitoring and visualization
5. **Backtesting framework** for strategy validation

## 🔗 Resources

- [Dune Analytics API Documentation](https://docs.dune.com/api-reference/)
- [Hyperliquid API Documentation](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api)
- [Plotly Python Documentation](https://plotly.com/python/)
- [Scikit-learn User Guide](https://scikit-learn.org/stable/user_guide.html)

---

<!-- [Raw Data Sources]
    ↓ Dune/Glassnode API    CoinGecko API     Social Media API/Scraper
    ↓                      ↓                  ↓
[Data Ingestion Scripts]   → Combine → [Feature Engineering Module]
                                    ↓
                          [Feature Store & Labeling]
                                    ↓
                          [Model Training & Evaluation]
                                    ↓
                        [Prediction & Screening Service]
                                    ↓
                          [Dashboard / Alerts System] -->

Phase 1: Planning & Data Collection
Define key objectives, target coins, and success metrics (e.g., identifying coins surviving pump-and-dumps).

Identify and connect to data sources:
On-chain analytics (Dune Analytics, Glassnode)
Price data (CoinGecko, CryptoDataDownload)
Social sentiment scraping/APIs (X/Twitter, Reddit)
Bot trading volume or exchange volume spikes
Build initial data ingestion scripts and store raw data locally or in a database.

Phase 2: Feature Engineering & Labeling
Preprocess and clean raw data, handle missing values.
Develop and compute composite on-chain, price, and sentiment features with appropriate time windows and aggregations.
Create labels for supervised learning (e.g., pump-and-dump survival, price movement classification).
Explore data visualization for feature understanding and correlation.

Phase 3: Model Development & Validation
Train baseline models (logistic regression, random forest) with cross-validation respecting temporal order.

Analyse feature importances and refine feature set.
Experiment with advanced models (gradient boosting, LSTM) for sequential pattern recognition.
Validate with backtesting on holdout periods, focusing on real-world profitability and false positive rates.

Phase 4: Deployment & Automation
Build pipeline to retrain models regularly and update features.
Develop alert/dashboard system highlighting buy signals and warnings from model predictions.
Automate data collection, prediction generation, and notifications (e.g., email, message).
Monitor model performance and drift, set up logging and error handling