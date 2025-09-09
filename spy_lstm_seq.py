import numpy as np
import pandas as pd
from datetime import time, timedelta

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()


def fetch_bar_data():
    # SQL query to fetch data from the database
    sql_query = r"""
    WITH bars_results AS (
    SELECT 
        req.id,
        req.http_response
    FROM (
        SELECT 
        id,
        (regexp_matches(url, '^https://data\.alpaca\.markets/v2/stocks/([^?]+)\?', 'i'))[1] AS request_type
        FROM http_request_log
    ) extracted
    JOIN http_request_log req ON req.id = extracted.id
    WHERE extracted.request_type = 'bars'
    ),

    flattened AS (
    SELECT 
        l.id,
        json_elem ->> 'o' AS open,
        json_elem ->> 'h' AS high,
        json_elem ->> 'l' AS low,
        json_elem ->> 'c' AS close,
        json_elem ->> 'v' AS volume,
        json_elem ->> 'n' AS number_of_trades,
        json_elem ->> 't' AS bar_timestamp,
        json_elem       AS full_json
    FROM bars_results l,
        LATERAL jsonb_array_elements(l.http_response::jsonb) AS json_elem
    ),

    flattened_typed AS (
    SELECT 
        id,
        open::numeric,
        high::numeric,
        low::numeric,
        close::numeric,
        volume::bigint,
        number_of_trades::bigint,
        CAST(bar_timestamp AS timestamptz) AS bar_ts,
        DATE(CAST(bar_timestamp AS timestamptz)) AS trade_date,
        (CAST(bar_timestamp AS timestamptz))::time AS trade_time  
    FROM flattened
    ),

    daily_close AS (
    SELECT DISTINCT ON (trade_date)
        trade_date,
        close AS close_at_4pm
    FROM flattened_typed
    WHERE trade_time <= '15:55:00'
    ORDER BY trade_date, trade_time DESC
    ),

    daily_close_lagged AS (
    SELECT 
        trade_date,
        LEAD(close_at_4pm) OVER (ORDER BY trade_date) AS next_day_close_at_4pm
    FROM daily_close
    ),
    daily_open AS (
    SELECT DISTINCT ON (trade_date)
        trade_date,
        open AS open_at_930am
    FROM flattened_typed
    WHERE trade_time = '09:30:00'
    ORDER BY trade_date
    )

    SELECT 
    f.*,
    (
        SUM(close::numeric * volume) OVER (
        PARTITION BY f.trade_date
        ORDER BY bar_ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) 
        /
        SUM(volume) OVER (
        PARTITION BY f.trade_date
        ORDER BY bar_ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ) AS vwap,
    ( close - LAG(close, 5) OVER (PARTITION BY f.trade_date ORDER BY bar_ts)) AS momentum_5,
    ( close - LAG(close, 10) OVER (PARTITION BY f.trade_date ORDER BY bar_ts)) AS momentum_10,
    (
        STDDEV(close) OVER (
        PARTITION BY f.trade_date
        ORDER BY bar_ts
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        )   
    ) AS stddev_10,
    (
        volume / AVG(volume) OVER (
        PARTITION BY f.trade_date
        ORDER BY bar_ts
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )  
    ) AS relative_volume_30,
        CASE WHEN f.trade_time >= TIME '09:30:00' AND f.trade_time <= TIME '16:00:00'
            THEN EXTRACT(HOUR FROM bar_ts) * 60 + EXTRACT(MINUTE FROM bar_ts) - 570
            ELSE NULL
        END AS minutes_since_open,
    ROW_NUMBER() OVER (PARTITION BY f.trade_date ORDER BY bar_ts) AS bar_num,
    MAX(high) OVER (PARTITION BY f.trade_date ORDER BY bar_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS high_since_open,
    MIN(low)  OVER (PARTITION BY f.trade_date ORDER BY bar_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS low_since_open,
    SUM(volume) OVER (PARTITION BY f.trade_date ORDER BY bar_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_volume,
    LEAD(close) OVER (PARTITION BY f.trade_date ORDER BY bar_ts) AS next_close,
    LEAD(close) OVER (PARTITION BY f.trade_date ORDER BY bar_ts) - close as next_bar_return,
    o.open_at_930am,  
    d.close_at_4pm,
    dl.next_day_close_at_4pm
    FROM flattened_typed f
    LEFT JOIN daily_open o ON f.trade_date = o.trade_date
    LEFT JOIN daily_close d ON f.trade_date = d.trade_date
    LEFT JOIN daily_close_lagged dl ON f.trade_date = dl.trade_date
    ORDER BY f.bar_ts;

    """


    # SQLAlchemy-compatible PostgreSQL URI
    db_url = (
        f"postgresql+psycopg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

    engine = create_engine(db_url)


    df = pd.read_sql_query(sql_query, engine, parse_dates=["bar_ts", "trade_date"])


    df['bar_ts'] = pd.to_datetime(df['bar_ts'])
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df['trade_time'] = pd.to_datetime(df['trade_time'], format='%H:%M:%S').dt.time

    return df

def filter_bar_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("bar_ts").copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df[df["close"].notnull()]  # Filter incomplete rows
    return df


def build_lstm_dataset(df, features, lookback_days=6, cutoff_time=time(12, 0), expected_bars_per_day=390, current_day_bars_cutoff=30):
    df = df.sort_values("bar_ts").copy()
    df = df.dropna(subset=features)
    trade_dates = sorted(df["trade_date"].unique())

    sequence_length = lookback_days * expected_bars_per_day + current_day_bars_cutoff

    X, y, date_labels = [], [], []

    for i in range(lookback_days, len(trade_dates) - 1):
        past_dates = trade_dates[i - lookback_days:i]
        target_date = trade_dates[i]

        past_data = df[df["trade_date"].isin(past_dates)]
        current_data = df[
            (df["trade_date"] == target_date) &
            (df["bar_ts"].dt.time <= cutoff_time)
        ]

        sequence_df = pd.concat([past_data, current_data])
        features_only = sequence_df[features]

        if len(features_only) < sequence_length:
            # pad short sequences
            pad_len = sequence_length - len(features_only)
            padded = np.pad(features_only.to_numpy(), ((pad_len, 0), (0, 0)), mode="constant", constant_values=0)
        elif len(features_only) > sequence_length:
            # truncate long sequences
            padded = features_only.to_numpy()[-sequence_length:]
        else:
            padded = features_only.to_numpy()

        target_val = df[df["trade_date"] == target_date]["close_at_4pm"].iloc[0]
        if pd.isnull(target_val):
            continue

        X.append(padded)
        y.append(target_val)
        date_labels.append(target_date)

    return np.array(X), np.array(y), date_labels



df = fetch_bar_data()
df = filter_bar_data(df)

# Drop forward-looking features
df = df.drop(columns=["next_close", "next_bar_return", "next_day_close_at_4pm"], errors='ignore')

# Fill or drop missing values
df["momentum_5"] = df["momentum_5"].fillna(0)
df["momentum_10"] = df["momentum_10"].fillna(0)
df["stddev_10"] = df["stddev_10"].bfill().fillna(0)

# Forward fill open_at_930am per day
df["open_at_930am"] = df.groupby("trade_date")["open_at_930am"].transform(lambda x: x.ffill().bfill())


# Fill minutes_since_open with -1 to indicate non-market hours
df["minutes_since_open"] = df["minutes_since_open"].fillna(-1)


# Write df to a CSV file
df.to_csv('spy_bars.csv', index=False)



features = [
    "open", "high", "low", "close", "volume",
    "vwap", "momentum_5", "momentum_10", "stddev_10",
    "relative_volume_30", "minutes_since_open",
    "high_since_open", "low_since_open", "cumulative_volume",
    "open_at_930am"
]

print(df['trade_date'].min(), df['trade_date'].max())
print(df['bar_ts'].head())
print(df[['trade_date', 'bar_ts', 'trade_time']].tail())

X, y, dates = build_lstm_dataset(df, features)
print(f"X shape: {X.shape}, y shape: {y.shape}")



def train_lstm_dataste(X, y):
    # Split the dataset
    X_train, X_test, y_train, y_test = train_test_split(X, y, shuffle=False, test_size=0.2)

    # Flatten for scaler, then reshape back
    num_samples_train, timesteps, num_features = X_train.shape
    scaler = StandardScaler()

    X_train_flat = X_train.reshape(-1, num_features)
    X_test_flat = X_test.reshape(-1, num_features)

    X_train_scaled = scaler.fit_transform(X_train_flat).reshape(num_samples_train, timesteps, num_features)
    X_test_scaled = scaler.transform(X_test_flat).reshape(X_test.shape[0], timesteps, num_features)

    model = Sequential([
        LSTM(128, input_shape=(timesteps, num_features), return_sequences=False),
        Dropout(0.3),
        Dense(64, activation='relu'),
        Dropout(0.2),
        Dense(1)  # Output: 4pm close
    ])

    model.compile(optimizer='adam', loss='mse', metrics=['mae'])
    model.summary()

    history = model.fit(
        X_train_scaled, y_train,
        validation_split=0.1,
        epochs=50,
        batch_size=32,
        verbose=1
    )

    # Evaluate performance
    loss, mae = model.evaluate(X_test_scaled, y_test)
    print(f"Test MAE: {mae:.4f}")

    # Predict
    y_pred = model.predict(X_test_scaled)

    # Plot
    plt.figure(figsize=(12, 5))
    plt.plot(y_test, label="Actual 4pm Close")
    plt.plot(y_pred, label="Predicted")
    plt.title("SPY 4pm Close Prediction")
    plt.xlabel("Sample")
    plt.ylabel("Price")
    plt.legend()
    plt.show()


train_lstm_dataste(X, y)
