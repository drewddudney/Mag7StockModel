#!/usr/bin/env python
# coding: utf-8

#Model Evaluation
# Script 4
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
#SQL 
from sqlalchemy import create_engine
import os
import psycopg2
#Modeling
import joblib
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine


# Pull values from SQL
def pull_from_sql(table_name="predicted_stock_prices"):
    try:
        #Create Engine
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        if not all([user, password, host, port, db_name]):
            raise ValueError("One or more environment variables are not set correctly.")

        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}")

        # Read data into DataFrame
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, engine)

        print("Successfully pulled SQL Data")
        return df

    except Exception as e:
        print(f"Error pulling data from SQL: {e}")
        return None


# Create two different filters for evaluation calculations
def filter_data(full_data):
    actual_data = full_data[['symbol', 'date', 'current_price']]
    predicted_data = full_data[['symbol', 'date', 'previous_day_predicted_price']]
    return actual_data, predicted_data


# Evaluate the Model
def eval_metrics(merged_data):
    metrics = []
    for symbol in merged_data['symbol'].unique():
        stock_data = merged_data[merged_data['symbol'] == symbol]
        
        r2 = r2_score(stock_data['current_price'], stock_data['previous_day_predicted_price'])
        mae = mean_absolute_error(stock_data['current_price'], stock_data['previous_day_predicted_price'])
        mse = mean_squared_error(stock_data['current_price'], stock_data['previous_day_predicted_price'])
        rmse = np.sqrt(mse)  # Ensure RMSE is calculated correctly
        directional_accuracy = np.mean(np.sign(stock_data['current_price'].diff().iloc[1:]) == np.sign(stock_data['previous_day_predicted_price'].diff().iloc[1:]))

        metrics.append({'symbol': symbol, 'r_squared': r2, 'mae': mae, 'rmse': rmse, 'mse': mse, 'directional_accuracy': directional_accuracy
        })
    
    return pd.DataFrame(metrics)


# Push back to SQL
def save_predictions_to_sql(predictions_df, table_name="model_eval"):
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

# Create Table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol TEXT,
            r_squared FLOAT,
            mae FLOAT,
            rmse FLOAT,
            mse FLOAT,
            directional_accuracy FLOAT,
            PRIMARY KEY (symbol)
        );
        """
        cursor.execute(create_table_query)

        # Insert Data
        for _, row in predictions_df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (symbol, r_squared, mae, rmse, mse, directional_accuracy)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO NOTHING;
            """
            cursor.execute(insert_query, (
                row['symbol'], row['r_squared'], row['mae'], row['rmse'], row['mse'], row['directional_accuracy']
            ))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully pushed to {table_name}")

    except Exception as e:
        print(f"Error inserting predictions into SQL: {e}")


# Main
def main():
    # Pull from SQL
    full_data = pull_from_sql()
    
    # Filter necessary columns
    actual_data, predicted_data = filter_data(full_data)
    merged_data = pd.merge(actual_data, predicted_data, on=['symbol', 'date'], how='inner')
    # Evaluation of model
    metrics_df = eval_metrics(merged_data)

    #SQL
    save_predictions_to_sql(metrics_df)

if __name__ == "__main__":
    main()


