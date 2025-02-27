#!/usr/bin/env python
# coding: utf-8

# Predictions
#Script 3, **Only Run After Obtaining Model files from Modeling.py**

import pandas as pd
#Models
import joblib
#SQL
from sqlalchemy import create_engine
import psycopg2
import os


#Pull values from SQL
def pull_from_sql(table_name="full_stock_calculations"):
    try:
        # Create engine
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}")

        # Read into DataFrame
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, engine)

        print("Successfully pulled SQL Data")
        return df

    except Exception as e:
        print(f"Error pulling data from SQL: {e}")
        return None
    finally:
        engine.dispose() 


#Make Predicitons of Next Day using saved Model files

def make_predictions(models, full_data):
    predictions = []

    for symbol, model in models.items():
        symbol_data = full_data[full_data['symbol'] == symbol]

        if not symbol_data.empty:
            input_features = symbol_data[['open_price', 'percent_change', 'cumulative_return', 
                                           'ma_10_day', 'ma_30_day', 'volatility_30_day', 
                                           'ema_10', 'ema_30']]
            symbol_prediction = model.predict(input_features)

            predictions.append(pd.DataFrame({
                'symbol': symbol,
                'predicted_price': symbol_prediction,
                'date': symbol_data['date']
            }))

    return pd.concat(predictions, ignore_index=True)


# Push back to SQL
def save_predictions_to_sql(predictions_df, table_name="predicted_stock_prices"):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname= os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        cursor = conn.cursor()

        # Create Table if doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol TEXT,
            predicted_price FLOAT,
            date DATE,
            current_price FLOAT,
            open_price FLOAT,
            percent_change FLOAT,
            cumulative_return FLOAT,
            ma_10_day FLOAT,
            ma_30_day FLOAT,
            volatility_30_day FLOAT,
            ema_10 FLOAT,
            ema_30 FLOAT,
            previous_day_predicted_price FLOAT,
            PRIMARY KEY (symbol, date)
        );
        """
        cursor.execute(create_table_query)

        # Insert Data
        for _, row in predictions_df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (symbol, predicted_price, date, current_price, open_price, percent_change, cumulative_return, ma_10_day, ma_30_day, volatility_30_day, ema_10, ema_30, previous_day_predicted_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) DO NOTHING;
            """
            cursor.execute(insert_query, (
                row['symbol'], row['previous_day_predicted_price'], row['date'],
                row['current_price'], row['open_price'], row['percent_change'],
                row['cumulative_return'], row['ma_10_day'], row['ma_30_day'],
                row['volatility_30_day'], row['ema_10'], row['ema_30'], row['predicted_price']
            ))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully pushed to {table_name}")

    except Exception as e:
        print(f"Error inserting predictions into SQL: {e}")


# Main
def main():
    full_data = pull_from_sql()
    symbols = ["AAPL", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "TSLA"]
    #Run model for all symbols
    models = {symbol: joblib.load(f'stock_price_predictor_{symbol}.joblib') for symbol in symbols}
    #DataFrame
    predictions_df = make_predictions(models, full_data)
    #Shift for current day price
    predictions_df['previous_day_predicted_price'] = predictions_df.groupby('symbol')['predicted_price'].shift(1)
    #Combine data into one df
    combined_data = pd.merge(full_data, predictions_df, on=['symbol', 'date'], how='left')
    combined_data = combined_data.drop(index=combined_data.index[:7])
    #SQL
    save_predictions_to_sql(combined_data)

if __name__ == "__main__":
    main()


