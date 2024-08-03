# method 2: window shifting method
# using the same ticker as the first example above
from itertools import chain

import numpy as np
import yfinance as yf
import matplotlib.dates as mpl_dates
import pandas as pd
from functions import  get_today
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mpl_dates
import matplotlib.pyplot as plt
from history import load_ticker_history_pd_frame
from functions import timestamp_to_datetime, human_readable_datetime
# get stock prices using yfinance library
def get_stock_price(ticker):
  df = yf.download(ticker, start=get_today({"test_mode":False},minus_days=90), threads= False)
  df['Date'] = pd.to_datetime(df.index)
  df['Date'] = df['Date'].apply(mpl_dates.date2num)
  df = df.loc[:,['Date', 'Open', 'High', 'Low', 'Close']]
  return df
# ticker = 'COST'
# df = get_stock_price(ticker)
def find_support_resistance_levels(ticker, ticker_history, module_config, flatten=False):
    # ticker = 'COST'
    # df = get_stock_price(ticker)
    # print(df)
    df = load_ticker_history_pd_frame(ticker, ticker_history[-module_config['sr_lookback']:],convert_to_datetime=True, human_readable=True)
    # print(df)
    df['date'] = pd.to_datetime(df.index)
    df['date'] = df['date'].apply(mpl_dates.date2num)
    df = df.loc[:, ['date', 'open', 'high', 'low', 'close']]
    #
    pivots = []
    max_list = []
    min_list = []
    for i in range(5, len(df) - 5):
        # taking a window of 9 candles
        high_range = df['high'][i - 5:i + 4]
        current_max = high_range.max()
        # if we find a new maximum value, empty the max_list
        if current_max not in max_list:
            max_list = []
        max_list.append(current_max)
        # if the maximum value remains the same after shifting 5 times
        if len(max_list) == 5 and is_far_from_level(current_max, pivots, df):
            pivots.append((high_range.idxmax(), current_max))

        low_range = df['low'][i - 5:i + 5]
        current_min = low_range.min()
        if current_min not in min_list:
            min_list = []
        min_list.append(current_min)
        if len(min_list) == 5 and is_far_from_level(current_min, pivots, df):
            pivots.append((low_range.idxmin(), current_min))
    pass
    pivots.sort(key=lambda x: x[1])
    _keys = [x[1] for x in pivots]
    n = 2
    if not flatten:
        return [_keys[i:i + n] for i in range(0, len(_keys), n)]
    else:
        flattened_levels = list(chain.from_iterable(pivots))
        flattened_levels.sort(key=lambda x: x)
        return flattened_levels
    # plot_all(pivots, df)
    pass

# method 1: fractal candlestick pattern
# determine bullish fractal
def is_support(df,i):
  cond1 = df['low'][i] < df['low'][i-1]
  cond2 = df['low'][i] < df['low'][i+1]
  cond3 = df['low'][i+1] < df['low'][i+2]
  cond4 = df['low'][i-1] < df['low'][i-2]
  return (cond1 and cond2 and cond3 and cond4)
# determine bearish fractal
def is_resistance(df,i):
  cond1 = df['high'][i] > df['high'][i-1]
  cond2 = df['high'][i] > df['high'][i+1]
  cond3 = df['high'][i+1] > df['high'][i+2]
  cond4 = df['high'][i-1] > df['high'][i-2]
  return (cond1 and cond2 and cond3 and cond4)
# to make sure the new level area does not exist already
def is_far_from_level(value, levels, df):
  ave =  np.mean(df['high'] - df['low'])
  return np.sum([abs(value-level)<ave for _,level in levels])==0
def plot_all(levels, df):
  fig, ax = plt.subplots(figsize=(16, 9))
  candlestick_ohlc(ax,df.values,width=0.6, colorup='green',
    colordown='red', alpha=0.8)
  # date_format = mpl_dates.DateFormatter('%Y-%m-%d %H:%M:%S')
  # ax.xaxis.set_major_formatter(date_format)
  for level in levels:
    plt.hlines(level[1], xmin = df['date'][level[0]], xmax =
      max(df['date']), colors='blue', linestyle='--')
  fig.show()

def find_nearest_support_level(sr_levels, ticker, ticker_history, module_config):
    return  max([i for i in sr_levels if ticker_history[-1].close > i])
    # below = max([i for i in myArr if myNumber > i])

def find_nearest_resistance_level(sr_levels, ticker, ticker_history, module_config):
    return  min([i for i in sr_levels if ticker_history[-1].close < i])