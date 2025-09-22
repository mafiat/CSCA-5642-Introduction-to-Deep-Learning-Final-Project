## Project
Stock Market Price & Direction Prediction (CSCA 5642 Final Project) — intraday SPY prediction using engineered technical features and an LSTM sequence model (noon → close formulation).

## Overview
This notebook implements an end-to-end pipeline to:
- fetch or load intraday OHLCV bars,
- engineer technical indicators (SMA, RSI, MACD, Bollinger Bands, momentum, etc.),
- compute a daily reference price (e.g., 11:00 / 12:00),
- build multi-day + partial-current-day sequences for an LSTM,
- scale data, train an LSTM regression to predict price change (reference → close),
- evaluate and visualize results (price, errors, basis points, directional accuracy).

## Key features
- Robust feature engineering for intraday bars
- Reference-price calculation and daily aggregation logic
- Sequence builder that assembles fixed-length LSTM inputs with padding/truncation
- End-to-end training, evaluation, and multiple plotting utilities
- Save/load dataset to/from CSV for reproducibility

## Requirements
Python packages used in the notebook:
- pandas, numpy, matplotlib
- scikit-learn
- tensorflow (Keras)
- alpaca-trade-api (or alpaca-py depending on usage)
- python-dotenv
(See cell 6 for the pip install command used during development.)

Environment variables:
- ALPACA_API_ID_KEY
- ALPACA_API_SECRET_KEY
Place them in a `.env` file or set them in your environment before fetching data.

## Installation
Install dependencies (example):
```bash
pip install alpaca-trade-api python-dotenv pandas numpy matplotlib scikit-learn tensorflow pandas_ta alpaca-py requests
```

Create a `.env` file at the repo root:
```
ALPACA_API_ID_KEY=your_api_key
ALPACA_API_SECRET_KEY=your_secret_key
```

## How to use (Notebook execution)
1. Open the Jupyter notebook.
2. Ensure the `.env` with Alpaca keys is available (if fetching data).
3. If using the provided CSV instead of API, update `csv_file_path` (cell 22).
4. Execute cells in order (the notebook is organized into logical sections) or run all:
    - Libraries / imports (cell 8)
    - Configure API / symbol / dates (cell 12)
    - Fetch data (cell 14) or load CSV (cell 22)
    - Feature engineering (cell 16)
    - Preprocessing (cells 30/31 and 32)
    - Reference-price calculation (cell 34 / 35)
    - Build sequences (cells 38, 42)
    - Split / scale / train / evaluate (cells 44, 47, 50, 54, 61, 64)
    - Visualizations and basis-point analysis (cells 27, 64, 66, 69)

Notes:
- If using Colab, optionally mount Google Drive (cell 10).
- The notebook contains defensive preprocessing steps to handle varied CSV exports (timestamp, Unnamed: 0, time formats).

## Files and important variables
- Notebook cells implement all pipeline steps; major variables:
  - stock_data / stock_data_preprocessed / stock_data_preprocessed_with_reference
  - X, y, dates, reference_prices (sequences and labels)
  - X_train_scaled, y_train_scaled, X_val_scaled, y_val_scaled, X_test_scaled, y_test_scaled
  - model, history, y_pred_changes, y_pred_close, y_test_close
- Saved CSV filename format: `{SYMBOL}_stock_data_{interval}{timeframe}_{start_date}_to_{end_date}.csv` (see cell 20).

## Tips & troubleshooting
- Missing columns/time format issues: ensure CSV includes `timestamp` or consistent `trade_date`/`trade_time`. Use the preprocessing cells that convert and fill values.
- If sequence builder reports zero sequences: verify there are enough trading days and that reference and close values exist for each day.
- For faster experimentation, start with daily or fewer-minute data and fewer lookback days.
- If Alpaca fetch fails, confirm API keys and paper/base URL; API changes may require updating client calls.

## Hyperparameters to tune
- lookback_days (sequence history length)
- cutoff_time (reference time for partial current-day bars)
- expected_bars_per_day / sequence length padding strategy
- LSTM architecture (units, layers, dropout)
- Optimizer learning rate, batch_size, epochs, early stopping patience

## Next steps / Improvements
- Add VWAP and trade counts, standardize bar counts per day, drop incomplete days.
- Try classification (up/down) or quantized regression for directional robustness.
- Use walk-forward cross-validation and economic metrics (backtest P&L, slippage, transaction costs).
- Experiment with Transformer/TCN models or ensembling.
- Add model & data pipeline logging, checkpointing, and drift monitoring for production.

## License
MIT

## Acknowledgements
This notebook was developed as part of CSCA 5642 — Introduction to Deep Learning final project.
Leverage some of the learning from @Pete Gordon
