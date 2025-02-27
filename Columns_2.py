#!/usr/bin/env python
# coding: utf-8

# Columns
# Script 2



import pandas as pd
#SQL
import psycopg2
from sqlalchemy import create_engine
import os


# Pull the stored data from SQL


# Using SQLAlchemy to pull data due to using Pandas
def pull_from_sql(table_name="stock_prices"):
    try:
        # Create engine
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}")

        # Read data
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, engine)

        print("Successfully pulled SQL Data")
        return df

    except Exception as e:
        print(f"Error pulling data from SQL: {e}")
        return None
    finally:
        engine.dispose() 


# Columns Added for Both Data Sets

def add_additional_features(cleaned_data):
    # Calculate daily returns
    cleaned_data['percent_change'] = round(cleaned_data.groupby('symbol')['current_price'].pct_change() * 100,2)

    # Calculate cumulative returns
    cleaned_data['cumulative_return'] = cleaned_data.groupby('symbol')['current_price'].transform(lambda x: (x / x.iloc[0]) - 1) * 100

    # Calculate moving averages
    cleaned_data['ma_10_day'] = cleaned_data.groupby('symbol')['current_price'].transform(lambda x: x.rolling(window=10).mean())
    cleaned_data['ma_30_day'] = cleaned_data.groupby('symbol')['current_price'].transform(lambda x: x.rolling(window=30).mean())

    # Calculate rolling volatility (standard deviation of returns)
    cleaned_data['volatility_30_day'] = cleaned_data.groupby('symbol')['percent_change'].transform(lambda x: x.rolling(window=30).std())

    # Calculate Exponential Moving Average (EMA)
    cleaned_data['EMA_10'] = cleaned_data.groupby('symbol')['current_price'].transform(lambda x: x.ewm(span=10, adjust=False).mean())
    cleaned_data['EMA_30'] = cleaned_data.groupby('symbol')['current_price'].transform(lambda x: x.ewm(span=30, adjust=False).mean())
    #Dropping first 210 due to NA values in those columns
    cleaned_data = cleaned_data.drop(index=cleaned_data.index[0:210]).reset_index(drop=True)

    return cleaned_data


#Push new calculated values back to SQL

def push_calculated_to_sql(df, table_name="full_stock_calculations"):
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

        # Create Table if it doesn't exist in SQL
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol TEXT,
            name TEXT,
            date DATE,
            current_price FLOAT,
            open_price FLOAT,
            percent_change FLOAT,
            cumulative_return FLOAT,
            ma_10_day FLOAT,
            ma_30_day FLOAT,
            volatility_30_day FLOAT,
            EMA_10 FLOAT,
            EMA_30 FLOAT,
            PRIMARY KEY (symbol, date) 
        );
        """
        cursor.execute(create_table_query)

        # Insert Data
        for _, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (symbol, name, date, current_price, open_price, percent_change, 
            cumulative_return, ma_10_day, ma_30_day, volatility_30_day, EMA_10, EMA_30)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) DO NOTHING;  -- Prevent duplicate entries
            """
            cursor.execute(insert_query, (
                row['symbol'], row['name'], row['date'],
                row['current_price'], row['open_price'],
                row['percent_change'], row['cumulative_return'],
                row['ma_10_day'], row['ma_30_day'],
                row['volatility_30_day'], row['EMA_10'], row['EMA_30']
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully pushed to {table_name}")

    except Exception as e:
        print(f"Error pushing calculated data to SQL: {e}")


# Main


def main():
    #Pull values, calculate new columns/features, push back to SQL
    full_data = pull_from_sql()
    back_to_sql = add_additional_features(full_data)
    push_calculated_to_sql(back_to_sql)
if __name__ == "__main__":
    main() 
