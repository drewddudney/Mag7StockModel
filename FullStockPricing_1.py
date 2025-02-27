#!/usr/bin/env python
# coding: utf-8

# Stock Pricing Pulls
# Script 1



#Imports
#Webscraping
import requests
from bs4 import BeautifulSoup
import random
import time
import pytz
#Data 
import pandas as pd
import numpy as np
from datetime import datetime
#SQL
import psycopg2
import os



#I personally like to extract to an HTML file to open in NoteBook++ to check out the format, already done.
#with open('yahoo_test.html','wb') as file:
   # file.write(soup.prettify('utf-8'))


# Pulls Current Day Pricing

def get_stock_info(stock_data):
    cst = pytz.timezone('America/Chicago')
    current_time = datetime.now(cst)
    if current_time.hour < 15:
        print("Stock data retrieval is only allowed after Close.")
        return stock_data 
    
    symbols = ["AAPL","AMZN","GOOGL","META","MSFT","NVDA","TSLA"]
    base_url = "https://finance.yahoo.com/quote/"
    headers = {"User-Agent": "Mozilla/5.0"}
    

    today_date = pd.Timestamp(datetime.today().date())

    for symbol in symbols:
        try:
            response = requests.get(f"{base_url}{symbol}/", headers=headers)
            time.sleep(random.uniform(10, 30))  # Random delay to prevent blocking
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error receiving data for {symbol}: {e}")
            continue  # Skip to the next symbol

        # Parse Website using lxml parser
        soup = BeautifulSoup(response.text, 'lxml')

        # Extracting, finding in HTML
        current_price_element = soup.find("span", {"data-testid": "qsp-price"})
        open_price_element = soup.find("fin-streamer", {"data-field": "regularMarketOpen"})
        name_element = soup.find("h1", {"class": "yf-xxbei9"})

        # Extract stock details with type conversion
        try:
            price = float(current_price_element.get_text(strip=True).replace(",", "")) if current_price_element else None
        except ValueError:
            price = None

        try:
            open_price = float(open_price_element.get_text(strip=True).replace(",", "")) if open_price_element else None
        except ValueError:
            open_price = None

        name = name_element.text.split('(')[0].strip() if name_element else "N/A"

        # Store data in a list of dictionaries, including the current date
        stock_data.append({
            "symbol": symbol,
            "name": name,
            "date": today_date,
            "current_price": price,
            "open_price": open_price,
        })

        print(f"Fetched data for {symbol}")

    return stock_data


# Pulls Historical Data From CSV
# Data Source:
# https://www.kaggle.com/datasets/unmoved/magnificent-7-past-10-years-prices-updated-daily



def clean_historical_data():
    # Load CSV data into a DataFrame
    df = pd.read_csv('HistoricalData.csv')

    df['Date'] = pd.to_datetime(df['Date'])

    # New DataFrame for final output
    temp_data = []
    symbol_to_name = {"AAPL": "Apple Inc.",
                      "AMZN": "Amazon.com, Inc.",
                      "GOOGL": "Alphabet Inc.",
                      "META": "Meta Platforms, Inc.",
                      "MSFT": "Microsoft Corporation",
                      "NVDA": "NVIDIA Corporation",
                      "TSLA": "Tesla, Inc."
                     }
   
    for symbol in df.columns[1:]:
        symbol_data = df[['Date', symbol]].copy() 
        symbol_data.rename(columns={symbol: 'current_price'}, inplace=True)

        # Add columns
        symbol_data['symbol'] = symbol
        symbol_data['current_price'] = round(symbol_data['current_price'],2)
        symbol_data['name'] = symbol_data['symbol'].map(symbol_to_name)
        symbol_data['open_price'] = round(symbol_data['current_price'].shift(1),2)
        symbol_data = symbol_data[['symbol', 'name', 'Date', 'current_price', 'open_price']]
        temp_data.append(symbol_data)

    # Concatenate all symbol data into a single DataFrame
    final_df = pd.concat(temp_data, ignore_index=True)

    # Rename 'Date' column to 'date'
    final_df.rename(columns={'Date': 'date'}, inplace=True)

    # Drop rows with NaN values
    final_df.dropna(subset=['current_price'], inplace=True)
    final_df.sort_values(by='date', inplace=True)
   
    # Drop the first row (earliest date) if it's the only row with NaN open prices
    final_df.reset_index(drop=True, inplace=True)

    return final_df


# Connecting to PostgreSQL
def push_to_sql(df, table_name="stock_prices"):
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
            name TEXT,
            date DATE,
            current_price FLOAT,
            open_price FLOAT,
            PRIMARY KEY (symbol, date)
        );
        """
        cursor.execute(create_table_query)

        # Insert Data
        for _, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (symbol, name, date, current_price, open_price)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) DO NOTHING;
            """
            cursor.execute(insert_query, (
                row['symbol'], row['name'], row['date'],
                row['current_price'], row['open_price']
            ))

        # Commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully pushed to {table_name}")

    except Exception as e:
        print(f"Error pushing data to SQL: {e}")


# Main

def main():
    # Pull current stock data
    stock_data = get_stock_info([])  # Initialize and populate stock_data
    stock_data_df = pd.DataFrame(stock_data)

    # Pull historical data
    historical_data = clean_historical_data()

    # Concatenate the two DataFrames
    full_data = pd.concat([historical_data, stock_data_df], ignore_index=True)

    # Upload to SQL
    push_to_sql(full_data)

if __name__ == "__main__":
    main() 

