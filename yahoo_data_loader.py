import datetime
import time
import traceback
from history import TickerHistory
from history import write_ticker_history_db_entries
from functions import obtain_db_connection, load_module_config, execute_query, read_csv
from indicators import process_ticker_history
import os
# from common import *

def download_raw_yahoo_data(ticker,module_config):
    # for ticker in module_config['tickers']:
    print(f"Downloading data for {ticker}")
    timestamp = int(int(time.mktime(datetime.datetime.now().timetuple())) * 1e3)
    timestamp = str(time.time()).split('.')[0]
    # print(f"curl -o data/yahoo/{ticker}.csv https://query1.finance.yahoo.com/v7/finance/download/SPY?period1=1185148800&period2=1724284800&interval=1d&events=history&includeAdjustedClose=true")
    # print(f"wget https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1=774921600&period2={datetime.datetime.now().timestamp}&interval=1d&events=history&includeAdjustedClose=true -P data/yahoo")
    url = f"curl -o data/yahoo/{ticker}.csv https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period2={timestamp}&period1=1185148800&interval=1d&events=history&includeAdjustedClose=true"
    print(url)
    os.system(url)
    time.sleep(2) #wait for download
def process_raw_yahoo_data(raw_data, module_config):
    #basically here convert realtime to usable data
    ticker_history=[]
    raw_data[0] = [x.lower() for x in raw_data[0]]
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
        tickers = module_config['tickers']
        print(f"Loading Ticker Data for {len(tickers)} tickers")
        for ticker in tickers:
            print(f"Loading data for {ticker}")
            download_raw_yahoo_data(ticker,module_config)
            raw_data = read_csv(f"{module_config['data_dir']}/{ticker}.csv")
            ticker_history = process_raw_yahoo_data(raw_data, module_config)
            write_ticker_history_db_entries(connection, ticker, ticker_history,module_config, auto_commit=True, cache=False)
        connection.close()
        # write_tickers_to_db(connection, tickers, module_config)
        # unloaded_tickers = []
        # rows = execute_query(connection, "select t.* from tickers_ticker t")
        # for i in range(1, len(rows)):
        #     entry = {}
        #     for ii in range(0, len(rows[0])):
        #         entry[rows[0][ii]] = rows[i][ii]
        #     unloaded_tickers.append(entry)
            # unloaded_tickers.append({x[]})

        # client = RESTClient(api_key=module_config['api_key'])
        # load_ticker_histories(unloaded_tickers)
        # if module_config['run_concurrently']:
        #     process_list_concurrently(unloaded_tickers, load_ticker_histories, int(len(unloaded_tickers)/module_config['num_processes'])+1)
        # else:
        #     load_ticker_histories([unloaded_tickers[-1]])
            # load_ticker_histories(unloaded_tickers)

        # combine_db_update_files(module_config)
        # execute_bulk_update_file(connection, module_config)

    except:
        traceback.print_exc()
        connection.close()
    print(f"\nCompleted MPB Initial Database Load in {int((int(time.time()) - start_time) / 60)} minutes and {int((int(time.time()) - start_time) % 60)} seconds")