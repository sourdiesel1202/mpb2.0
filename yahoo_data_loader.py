import datetime
import json
import time
import traceback
from history import TickerHistory
from history import write_ticker_history_db_entries
from functions import obtain_db_connection, load_module_config, execute_query, read_csv, write_csv
from indicators import process_ticker_history
import os
import yfinance as yf
# from common import *

def download_raw_yahoo_data(ticker,module_config):
    t = yf.Ticker(ticker)
    history = t.history(start=(datetime.datetime.now()-datetime.timedelta(days=365*30)).strftime("%Y-%m-%d"), end=datetime.datetime.now().strftime('%Y-%m-%d'), interval="1d")
    #ok so processing this is going to be a pain in the ass
    rows= [['Date', 'Open','Close','High','Low','Volume']]
    for i in range(0,len(history.index.values)):
        #ok so write the csv here
        row = [str(history.index.values[i]).split('T')[0],history['Open'][i],history['Close'][i],history['High'][i], history['Low'][i], history['Volume'][i]]
        rows.append(row)
        # str(history['Open'].index.values[-1]).split('T')[0]
        pass
    write_csv(f"data/yahoo/{ticker}.csv", rows)
    pass

    # for ticker in module_config['tickers']:
    # if os.path.exists(f"data/yahoo/{ticker}.csv"):
    #     returnhistory
    # print(f"Downloading data for {ticker}")
    # # timestamp = int(int(time.mktime(datetime.datetime.now().timetuple())) * 1e3)
    # timestamp = str(time.time()).split('.')[0]
    # url = f"curl -o data/yahoo/{ticker}.csv https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period2={timestamp}&period1=774921600&interval=1d&events=history&includeAdjustedClose=true"
    # print(url)
    # os.system(url)
    # time.sleep(2) #wait for download
def process_raw_yahoo_data(raw_data, module_config):
    #basically here convert realtime to usable data
    ticker_history=[]
    if len(raw_data) < 2:
        return []
    raw_data[0] = [x.lower() for x in raw_data[0]]
    print(f"Oldest history record: {raw_data[1][0]} Latest history record: {raw_data[-1][0]} Total Records: {len(raw_data)-1}")
    for i in range(1, len(raw_data)):
        #xconvert to timestamp
        timestamp = int(int(time.mktime(datetime.datetime.strptime(f"{raw_data[i][raw_data[0].index('date')]} 15:30:00", "%Y-%m-%d %H:%M:%S").timetuple()))*1e3)

        round(12.40, ndigits=2)
        # def __init__(self, open, close, high, low, volume, timestamp):
        ticker_history.append(TickerHistory(raw_data[i][raw_data[0].index('open')],raw_data[i][raw_data[0].index('close')], raw_data[i][raw_data[0].index('high')], raw_data[i][raw_data[0].index('low')], raw_data[i][raw_data[0].index('volume')], timestamp))
    return  ticker_history
    pass
if __name__ == '__main__':

    start_time = time.time()
    module_config = load_module_config("yahoo_data_loader")
    connection = obtain_db_connection(module_config)

    try:
        print(f"Loading Ticker Data for {len(module_config['tickers'])} tickers")
        # for ticker in tickers:
        for i in range(0, len(module_config['tickers'])):
            ticker=module_config['tickers'][i]
            print(f"Loading data for {ticker}")
            download_raw_yahoo_data(ticker,module_config)
            raw_data = read_csv(f"{module_config['data_dir']}/{ticker}.csv")
            ticker_history = process_raw_yahoo_data(raw_data, module_config)
            write_ticker_history_db_entries(connection, ticker, ticker_history,module_config, auto_commit=True, cache=False)
            # os.system(f"rm data/yahoo/{ticker}.csv")
        connection.close()

    except:
        traceback.print_exc()
        connection.close()
    print(f"\nCompleted MPB Initial Database Load in {int((int(time.time()) - start_time) / 60)} minutes and {int((int(time.time()) - start_time) % 60)} seconds")