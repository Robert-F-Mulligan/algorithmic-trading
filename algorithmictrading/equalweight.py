# equalweight.py

import numpy as np
import pandas as pd
import requests
import xlsxwriter
import os
from os import path
from secrets import IEX_CLOUD_API_TOKEN

def make_chunks(df):
     return np.array_split(df['Ticker'].to_list(), np.ceil(len(df) / 100))

def get_data_batch(df):
    df_list = []
    chunks = make_chunks(df)
    for chunk in chunks:
        ticker_strings = ','.join(chunk)
        batch_api_call_url = f'https://sandbox.iexapis.com/stable/stock/market/batch/?types=stats,quote&symbols={ticker_strings}&token={IEX_CLOUD_API_TOKEN}'
        data = requests.get(batch_api_call_url).json()
        tickers = [k for k in data.keys()]
        latestprices = [data[k]['quote']['latestPrice'] for k in data.keys()]
        marketcaps = [data[k]['quote']['marketCap'] for k in data.keys()]
        df = pd.DataFrame({'ticker': tickers, 'latest_price': latestprices, 'market_cap': marketcaps})
        df_list.append(df)
    return  pd.concat(df_list, ignore_index=True)

# break up get_data_batch to individual transfomations per portfolio

def get_share_amounts(df, portfolio_size=50000000):
    share_amounts = portfolio_size / len(df.index)
    return df.assign(recommended_trades= lambda x: np.floor(share_amounts /  x['latest_price']))

def make_equal_weight_portfolio(df, portfolio_size):
    return (df.copy()
              .pipe(get_data_batch)
              .pipe(get_share_amounts, portfolio_size=portfolio_size)
    )

if __name__ == "__main__":
    portfolio_size = 40000000

    sp500 = pd.read_csv(r'..\data\sp_500_stocks.csv')

    df = make_equal_weight_portfolio(sp500, portfolio_size)

    print(df.head())


    
