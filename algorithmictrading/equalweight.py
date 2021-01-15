# equalweight.py

import numpy as np
import pandas as pd
import requests
import xlsxwriter
import os
from secrets import IEX_CLOUD_API_TOKEN

def make_chunks(df):
     return np.array_split(df['Ticker'].to_list(), np.ceil(len(df) / 100))

def make_batch_api_call(string_chunk):
    ticker_strings = ','.join(string_chunk)
    batch_api_call_url = f'https://sandbox.iexapis.com/stable/stock/market/batch/?types=stats,quote,advanced-stats&symbols={ticker_strings}&token={IEX_CLOUD_API_TOKEN}'
    return requests.get(batch_api_call_url).json()

def get_equal_weight_data(df):
    df_list = []
    chunks = make_chunks(df)
    for chunk in chunks:
        data = make_batch_api_call(chunk)
        tickers = [k for k in data.keys()]
        latestprices = [data[k]['quote']['latestPrice'] for k in data.keys()]
        marketcaps = [data[k]['quote']['marketCap'] for k in data.keys()]
        df = pd.DataFrame({'ticker': tickers, 'latest_price': latestprices, 'market_cap': marketcaps})
        df_list.append(df)
    return  pd.concat(df_list, ignore_index=True)

def get_momentum_data(df):
    df_list = []
    chunks = make_chunks(df)
    for chunk in chunks:
        data = make_batch_api_call(chunk)
        tickers = [k for k in data.keys()]
        latestprices = [data[k]['quote']['latestPrice'] for k in data.keys()]
        one_year_changes = [data[k]['stats']['year1ChangePercent'] for k in data.keys()]
        six_month_changes = [data[k]['stats']['month6ChangePercent'] for k in data.keys()]
        three_month_changes = [data[k]['stats']['month3ChangePercent'] for k in data.keys()]
        one_month_changes = [data[k]['stats']['month1ChangePercent'] for k in data.keys()]
        df = pd.DataFrame({'ticker': tickers, 
                           'latest_price': latestprices, 
                           '1_year_change': one_year_changes,
                           '6_month_change': six_month_changes,
                           '3_month_change': three_month_changes,
                           '1_month_change': one_month_changes
                          })
        df_list.append(df)
    return  pd.concat(df_list, ignore_index=True)

def value_df_transform(df):
    return (df.copy()
              .assign(enterpriseValue_EBITDA= lambda x: x['enterpriseValue'] / x['EBITDA'],
                      enterpriseValue_grossProfit= lambda x: x['enterpriseValue'] / x['grossProfit'])
              .drop(columns=['enterpriseValue', 'EBITDA', 'grossProfit'])
              .rename(columns={'enterpriseValue_EBITDA': 'enterpriseValue/EBITDA',
                               'enterpriseValue_grossProfit' : 'enterpriseValue/grossProfit'})
           )

def get_value_data(df):
    df_list = []
    chunks = make_chunks(df)
    for chunk in chunks:
        data = make_batch_api_call(chunk)
        tickers = [k for k in data.keys()]
        latestprices = [data[k]['quote']['latestPrice'] for k in data.keys()]
        pe_ratios = [data[k]['quote']['peRatio'] for k in data.keys()]
        price_books = [data[k]['advanced-stats']['priceToBook'] for k in data.keys()]
        price_sales = [data[k]['advanced-stats']['priceToSales'] for k in data.keys()]
        enterpriseValues = [data[k]['advanced-stats']['enterpriseValue'] for k in data.keys()]
        ebitas = [data[k]['advanced-stats']['EBITDA'] for k in data.keys()]
        grossprofits = [data[k]['advanced-stats']['grossProfit'] for k in data.keys()]
        df = (pd.DataFrame({'ticker': tickers, 
                           'latest_price': latestprices, 
                           'peRatio': pe_ratios,
                           'priceToBook': price_books,
                           'priceToSales': price_sales,
                           'enterpriseValue': enterpriseValues,
                           'EBITDA': ebitas,
                           'grossProfit': grossprofits
                          })
              .pipe(value_df_transform)
             )
        df_list.append(df)
    return  pd.concat(df_list, ignore_index=True)

def generate_high_quality_momentum_score(df, stock_cutoff=50):
    return (df.copy()
              .set_index(['ticker', 'latest_price'])
              .rank(pct=True)
              .assign(hqm_score= lambda x: x.mean(axis='columns'))
              .sort_values('hqm_score', ascending=False)
              .reset_index()
              .head(stock_cutoff)
           )

def fill_missing_vals(df):
    df = df.copy()
    return (df.set_index('ticker')
              .apply(lambda x: x.fillna(x.mean()))
              .reset_index()
           )

def generate_robust_value_score(df, stock_cutoff=50):
    return (df.copy()
              .set_index(['ticker', 'latest_price'])
              .rank(pct=True)
              .assign(rv_score= lambda x: x.mean(axis='columns'))
              .sort_values('rv_score')
              .reset_index()
              .head(stock_cutoff)
           )

def get_share_amounts(df, portfolio_size=50000000):
    share_amounts = portfolio_size / len(df.index)
    return df.assign(recommended_trades= lambda x: np.floor(share_amounts /  x['latest_price']))

def make_equal_weight_portfolio(df, portfolio_size):
    return (df.copy()
              .pipe(get_equal_weight_data)
              .pipe(get_share_amounts, portfolio_size=portfolio_size)
    )

def make_momentum_portfolio(df, portfolio_size, stock_cutoff=50):
    return (df.copy()
              .pipe(get_momentum_data)
              .pipe(generate_high_quality_momentum_score, stock_cutoff=stock_cutoff)
              .pipe(get_share_amounts, portfolio_size=portfolio_size)
    )

if __name__ == "__main__":
    portfolio_size = 40000000

    sp500 = pd.read_csv(r'..\data\sp_500_stocks.csv')

    df = make_equal_weight_portfolio(sp500, portfolio_size)
    df2 = make_momentum_portfolio(sp500, portfolio_size)

    print(df.head())
    print(df2.head())