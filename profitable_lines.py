import json
import multiprocessing
import os
import time
import traceback

# from shapesimilarity import shape_similarity
# import matplotlib.pyplot as plt
import numpy as np
from statistics import mean

from tickers import load_ticker_symbol_by_id, load_ticker_history_by_id
from functions import execute_query, execute_update, timestamp_to_datetime, human_readable_datetime, \
    calculate_x_is_what_percentage_of_y, process_list_concurrently
from history import load_ticker_history_db, TickerHistory, load_ticker_history_cached
from shape import compare_tickers_at_index, determine_line_similarity


def dump_profitable_line_cache(connection, module_config):
    # write_csv(f"{module_config['output_dir']}cached/{ticker}{module_config['timespan_multiplier']}{module_config['timespan']}.csv",convert_ticker_history_to_csv(ticker, ticker_history))
    line_matrix = load_profitable_line_matrix(connection, module_config, ignore_cache=False)
    with open(f"{module_config['output_dir']}cached/profitable_line_cache.json", "w+") as f:
        f.write(json.dumps(line_matrix))

def load_profitable_line_matrix(connection,  module_config, ignore_cache=False):
    #load the profitable line matrix
    if not ignore_cache:
        if os.path.exists(f"{module_config['output_dir']}cached/profitable_line_cache.json"):
            print(f"Loading profitable line matrix from cache")
            with open(f"{module_config['output_dir']}cached/profitable_line_cache.json", "r") as f:
                return json.loads(f.read())

    execute_update(connection, f"SET SESSION group_concat_max_len = 100000000",auto_commit=True)
    # rows = execute_query(connection, f"select pl.id, forward_range, backward_range, profit_percentage,plt.id line_type_id,plt.name line_type_name,count(distinct plh.tickerhistory_id) matches ,group_concat(concat(th.ticker_id,':',plh.tickerhistory_id) separator ',') ticker_histories from lines_profitableline pl, (select profitableline_id, max(tickerhistory_id) tickerhistory_id from lines_profitableline_histories  group by profitableline_id) plh,history_tickerhistory th, lines_profitablelinetype plt  where  plt.id =pl.line_type_id and th.id=plh.tickerhistory_id and plh.profitableline_id = pl.id and th.timespan='{module_config['timespan']}' and th.timespan_multiplier='{module_config['timespan_multiplier']}' GROUP by pl.id, forward_range, backward_range, profit_percentage,plt.id,plt.name")
    #rows = execute_query(connection, f"select pl.id, forward_range, backward_range, profit_percentage,plt.id line_type_id,plt.name line_type_name,count(distinct plh.tickerhistory_id) matches ,group_concat(concat(th.ticker_id,':',t.symbol,':',th.timestamp,':',plh.tickerhistory_id) separator ',') ticker_histories from lines_profitableline pl,tickers_ticker t , (select profitableline_id, max(tickerhistory_id) tickerhistory_id from lines_profitableline_histories  group by profitableline_id) plh,history_tickerhistory th, lines_profitablelinetype plt  where t.id=th.ticker_id and plt.id =pl.line_type_id and th.id=plh.tickerhistory_id and plh.profitableline_id = pl.id and th.timespan='{module_config['timespan']}' and th.timespan_multiplier='{module_config['timespan_multiplier']}' GROUP by pl.id, forward_range, backward_range, profit_percentage,plt.id,plt.name")
    # remove above cause I think it's fucked
    rows = execute_query(connection, f"select pl.id, forward_range, backward_range, profit_percentage,plt.id line_type_id,plt.name line_type_name, group_concat(concat(th.ticker_id,':',t.symbol,':',th.timestamp,':',plh.tickerhistory_id) separator ',') ticker_histories from lines_profitableline pl,tickers_ticker t, (select profitableline_id, max(tickerhistory_id) tickerhistory_id from (select phuck.profitableline_id, phuck.tickerhistory_id, th.timestamp from  lines_profitableline_histories phuck, history_tickerhistory th,  (select plh.profitableline_id, max(th.timestamp) most_recent_timestamp from lines_profitableline_histories plh, history_tickerhistory th where th.id=plh.tickerhistory_id group by plh.profitableline_id) fuck where phuck.profitableline_id=fuck.profitableline_id and th.timestamp=fuck.most_recent_timestamp and th.id=phuck.tickerhistory_id) dumb group by dumb.profitableline_id) plh,history_tickerhistory th, lines_profitablelinetype plt  where t.id=th.ticker_id and plt.id =pl.line_type_id and th.id=plh.tickerhistory_id and plh.profitableline_id = pl.id and th.timespan='{module_config['timespan']}' and th.timespan_multiplier='{module_config['timespan_multiplier']}' GROUP by pl.id, forward_range, backward_range, profit_percentage,plt.id,plt.name")

    results = {}
    for i in range(1, len(rows)):
        # if int(rows[i][rows[0].index('matches')])  < 100: #basically skip any that we've found that aren't really relevant yet
        #     continue
        try:
            results[rows[i][rows[0].index('line_type_name')]] = {
                "profit": int(float(rows[i][rows[0].index('profit_percentage')])),
                "matches" :[{x.split(":")[-1]:x.split(":")[0]} for x in rows[i][rows[0].index('ticker_histories')].split(',')],
                'symbol': [x.split(":")[1] for x in rows[i][rows[0].index('ticker_histories')].split(',')][0],
                'timestamp': [x.split(":")[2] for x in rows[i][rows[0].index('ticker_histories')].split(',')][0],
                "forward_range": int(float(rows[i][rows[0].index('forward_range')])),
                "backward_range": int(float(rows[i][rows[0].index('backward_range')]))
            }
        except:
            traceback.print_exc()
            print(f"Cannot load profitable line matrix entry {rows[i]}")
            raise  Exception(f"Cannot load profitable line matrix entry {rows[i]}")

        pass
    return results
def update_ticker_profitable_lines(connection,profitable_line,ticker, ticker_history, profitable_lines, module_config):

    matches = []
    for indexes in profitable_lines.values():
        for index in indexes:
            if index not in matches:
                matches.append(index)
    #ok so lock the rows first
    execute_query(connection, f"select * from lines_profitableline_histories where profitableline_id = (select id from lines_profitableline where line_type_id=(select id from lines_profitablelinetype where name='{profitable_line}'))", verbose=False)
    for index in matches:
        execute_update( connection, f"insert ignore into lines_profitableline_histories (profitableline_id, tickerhistory_id) values ((select id from lines_profitableline where line_type_id=(select id from lines_profitablelinetype where name='{profitable_line}')),(select id from history_tickerhistory where timestamp={ticker_history[index].timestamp} and timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}' and ticker_id=(select id from tickers_ticker where symbol='{ticker}')))", verbose=False, auto_commit=False)
    connection.commit()
def write_ticker_profitable_lines(connection, ticker, ticker_history, profitable_lines, module_config):
    #ok so the idea where is that we write the line first and then
    print(f"writing profitable lines for {ticker}")
    for profit_percentage, indexes in profitable_lines.items():
        if len(indexes) >= module_config['line_profit_minimum_matches']:
            new_line_str = f"Profitable Line {str(time.time()).split('.')[0]}{os.getpid()}"
            execute_query(connection, f"select * from lines_profitablelinetype where name='{new_line_str}' ")
            execute_update(connection, sql=f"insert ignore into lines_profitablelinetype (name) values ('{new_line_str}')", auto_commit=True, verbose=False, cache=False)
            execute_query(connection, "select * from lines_profitableline")

            execute_update(connection, sql=f"insert ignore into lines_profitableline (line_type_id, forward_range, backward_range, profit_percentage) values ((select id from lines_profitablelinetype where name='{new_line_str}' ),{module_config['line_profit_forward_range']},{module_config['line_profit_backward_range']},{int(profit_percentage)})", auto_commit=True, verbose=False,cache=False)

            execute_query(connection,f"select * from lines_profitableline_histories where profitableline_id = (select id from lines_profitableline where line_type_id=(select id from lines_profitablelinetype where name='{new_line_str}'))",verbose=False)

            for index in indexes:
                execute_update(connection,f"insert ignore into lines_profitableline_histories (profitableline_id, tickerhistory_id) values ((select id from lines_profitableline where forward_range={module_config['line_profit_forward_range']} and backward_range={module_config['line_profit_backward_range']} and profit_percentage={int(profit_percentage)}), (select id from history_tickerhistory where timespan='{module_config['timespan']}' and timespan_multiplier='{module_config['timespan_multiplier']}' and timestamp={ticker_history[index].timestamp} and ticker_id=(select id from tickers_ticker where symbol='{ticker}')))",auto_commit=False, verbose=False,cache=False)
            connection.commit()
            time.sleep(1)
    pass
    connection.commit()

def compare_profitable_ticker_lines_to_market(connection,ticker, ticker_history, module_config, read_only=True):
    '''
    ok so basically if we set read only we just return the matching profitable line, None if not
    :param connection:
    :param ticker:
    :param ticker_history:
    :param module_config:
    :param read_only:
    :return:
    '''
    # /find_ticker_profitable_lines(ticker, load_ticker_history_db(ticker, module_config, connection=connection), module_config)
    pass
    ticker_profitable_lines = find_ticker_profitable_lines(ticker, ticker_history,module_config)
    profitable_line_matrix = load_profitable_line_matrix(connection, module_config, ignore_cache=True)
    pass
    loaded_histories ={}
    print(f"Found {len(ticker_profitable_lines)} Ticker Profitable lines")
    #ok so the idea here is now to iterate over the genereated profitable lines of the ticker to the profitable lines of the market
    for profit, indexes in ticker_profitable_lines.items():
        positive_profit = profit >=0
        matches = {}#keys of percentage matched vs the name of the line matched
        profits = {}
        #basically we can just compare the first to the first for now
        for line, line_data in profitable_line_matrix.items():
            positive_line_data_profit = line_data['profit'] >= 0
            #so here we load the ticker histroy
            if positive_profit != positive_line_data_profit:
                print(f"Skipping Historic Profit Line of {line_data['profit']}%, the trend is in the opposite direction of profit {profit}%")
                continue
            try:
                # compare_ticker = load_ticker_symbol_by_id(connection, [x for x in line_data['matches'][0].values()][0], module_config)
                compare_ticker = line_data['symbol']
            except:
                traceback.print_exc()
                print()
                raise Exception
            if compare_ticker not in loaded_histories:

                loaded_histories[compare_ticker]=load_ticker_history_cached(compare_ticker, module_config)
            #ok so now we need to load the ticker history entry at the matching id
            # history_entry = load_ticker_history_by_id(connection, [x for x in line_data['matches'][0].keys()][0],compare_ticker, module_config)
            history_entry = [x for x in loaded_histories[compare_ticker] if x.timestamp == int(line_data['timestamp'])][0]
            compare_index = next((i for i, item in enumerate(loaded_histories[compare_ticker]) if item.timestamp == history_entry.timestamp), -1)
            _ticker_history, loaded_histories[compare_ticker] = normalize_prices(ticker_history, loaded_histories[compare_ticker],module_config)
            try:
                if len(_ticker_history[0:indexes[0]]) >= module_config['line_profit_backward_range'] and len(loaded_histories[compare_ticker]) >= module_config['line_profit_backward_range']:
                    match_likelihood = compare_tickers_at_index(compare_index, ticker, _ticker_history[0:indexes[0]], compare_ticker, loaded_histories[compare_ticker], module_config)
                    matches[match_likelihood] = line
                    # profits[match_likelihood]= line_data['profit']

            except:
                # traceback.print_exc()
                # print(f"Cannot calculate shape similarity between {ticker} (size: {len(ticker_history[0:indexes[0]])}) and {compare_ticker} (size: {len(loaded_histories[compare_ticker])}), not enough historical bars ")
                continue

        valid_matches = [x for x in matches.keys() if x > module_config['line_similarity_gt']]
        if len(valid_matches) > 0:
            #if we have more than one, we use the most accurate
            # def write_ticker_profitable_lines(connection, ticker, ticker_history, profitable_lines, module_config):
            if not read_only:
                print(f"Updating ticker lines for {matches[max(valid_matches)]}")
                update_ticker_profitable_lines(connection, matches[max(valid_matches)], ticker, ticker_history, {profit:indexes}, module_config)
            return
            pass
        else: #otherwise we need to write the profitable lines for the ticker itself
            print(f"Did not find any matches")
            if not read_only:
                write_ticker_profitable_lines(connection, ticker, ticker_history, {profit:indexes}, module_config)
            pass
            #ok so once we're here, we can calculate the similarity at the index
            #the challenge is finding the index of the ticker history
        pass


# def scrub_potential_profitable_line_indexes(ticker, ticker_history, i, indexes, module_config):
def scrub_potential_profitable_line_indexes(kwarg):
    '''
    blahhhhhhhh just more performance improvements using subprocess
    The idea here is we'll process a subset of indexes passed in from scrub_potential_profitable_lines,
    once processed, write them locally to json and then load them back into the main process (although, when concurrent this
    is the child process)

    :param ticker:
    :param ticker_history:
    :param i:
    :param indexes:
    :param module_config:
    :return:
    '''
    main_indexes = kwarg['main_indexes']
    test_string = kwarg['test_string']
    indexes = kwarg['indexes']
    ticker = kwarg['ticker']
    ticker_history = kwarg['ticker_history']
    # i = kwarg['i']
    module_config=kwarg['module_config']
    index_dict = {}
    for i in range(0, len(main_indexes)):

        if main_indexes[i] not in index_dict:  # first pass on the indexes to get n
            index_dict[indexes[i]] = {}
        for ii in range(0, len(indexes)):  # second pass on indexes to get comparison
            if i == ii:
                index_dict[indexes[ii]] = round(1.00, 2)

            ticker_history_a, ticker_history_b = normalize_prices(
                ticker_history[indexes[i] - module_config['line_profit_backward_range']:indexes[i] + 1],
                ticker_history[indexes[ii] - module_config['line_profit_backward_range']:indexes[ii] + 1],
                module_config)
            # print(f"Testing {i}/{len(indexes)} ${ticker}:{ticker_history[indexes[i]].dt}:last price ${ticker_history[indexes[i]].close}: => {ii}/{len(indexes)}  ${ticker_history[indexes[ii]].dt}:last price ${ticker_history[indexes[i]].close}: {similarity}")
            similarity = determine_line_similarity(ticker_history_a, ticker_history_b, module_config)
            index_dict[indexes[ii]] = round(similarity, 2)
            print(f"{test_string}:Testing {i}/{len(main_indexes)}:{ii}/{len(indexes)} ${ticker}:{ticker_history_a[-1].dt}:Control Date : {ticker_history[indexes[i]].dt} => {ii}/{len(indexes)}  ${ticker}:{ticker_history_b[-1].dt}:Control Date: {ticker_history[indexes[ii]].dt}: Similarity: {similarity}")

    with open(f"{module_config['output_dir']}cached/PL/{ticker}{module_config['timespan_multiplier']}{module_config['timespan']}PL{os.getpid()}.json", "w+") as f:
        f.write(json.dumps(index_dict))
def scrub_potential_profitable_lines(ticker, ticker_history,potential_profitable_lines, module_config):

    cleanup_dict= {}
    for profit_value, indexes in potential_profitable_lines.items():
        index_dict = {}
        # f"")
        for i in range(0, len(indexes)):

            if indexes[i] not in index_dict:  # first pass on the indexes to get n
                index_dict[indexes[i]] = {}
            for ii in range(0, len(indexes)):  # second pass on indexes to get comparison
                if i == ii:
                    index_dict[indexes[ii]] = round(1.00, 2)

                ticker_history_a, ticker_history_b = normalize_prices(
                    ticker_history[indexes[i] - module_config['line_profit_backward_range']:indexes[i] + 1],
                    ticker_history[indexes[ii] - module_config['line_profit_backward_range']:indexes[ii] + 1],
                    module_config)
                # print(f"Testing {i}/{len(indexes)} ${ticker}:{ticker_history[indexes[i]].dt}:last price ${ticker_history[indexes[i]].close}: => {ii}/{len(indexes)}  ${ticker_history[indexes[ii]].dt}:last price ${ticker_history[indexes[i]].close}: {similarity}")
                try:
                    similarity = determine_line_similarity(ticker_history_a, ticker_history_b, module_config)
                    index_dict[indexes[ii]] = round(similarity, 2)
                    # print(f"Profit Index {[x for x in potential_profitable_lines.keys()].index(profit_value)}/{len([x for x in potential_profitable_lines.keys()])}: {profit_value}%: Testing {i}/{len(indexes)}:{ii}/{len(indexes)} ${ticker}:{ticker_history_a[-1].dt}:Control Date : {ticker_history[indexes[i]].dt} => {ii}/{len(indexes)}  ${ticker}:{ticker_history_b[-1].dt}:Control Date: {ticker_history[indexes[ii]].dt}: Similarity: {similarity}")
                except:
                    traceback.print_exc()
                    index_dict[indexes[ii]] = round(0.00, 2)
        #ok so now that we're here, let's go ahead and process the individual lines
        # _pids = []
        # index_dict = {} #THIS is where things are going to get complicado
        # _keys = [x for x in indexes]
        # # n = int(module_config['num_processes']/12)+1
        # n = int(len(indexes) / 2) + 1
        #
        # # def scrub_potential_profitable_line_indexes(ticker, ticker_history, i, indexes, module_config):
        # # _keys = [x for x in data]
        # # n = batch_size
        # index_loads = [_keys[i:i + n] for i in range(0, len(_keys), n)]
        # # for _i in range(0, len(index_loads)):
        # #     if indexes[i] not in index_loads[_i]:
        # #         index_loads[_i].append(indexes[i]) #quick hack for that index bug
        # loads = [{"test_string":f'Load {x}/{len(index_loads)}', "ticker": ticker, "module_config": module_config, "ticker_history": ticker_history, "main_indexes":index_loads[x], "indexes":indexes} for x in range(0, len(index_loads))]
        # processes = {}
        # for load in loads:
        #     p = multiprocessing.Process(target=scrub_potential_profitable_line_indexes, args=(load,))
        #     p.start()
        #     processes[str(p.pid)] = p
        # pids = [x for x in processes.keys()]
        # while any(processes[p].is_alive() for p in processes.keys()):
        #     pass
        #     # print(f"Waiting for {len([x for x in processes if x.is_alive()])} processes to complete. Going to sleep for 10 seconds")
        #     # process_str = ','.join([str(v.pid) for v in processes.values() if v.is_alive()])
        #     # print(f"The following child processes are still running: {process_str}")
        #     # time.sleep(0.5)
        # #ok so once we are here, let's go ahead and load the index dict
        # for pid in pids:
        # # for pid in _pids:
        #     # _pids.append(pid)
        #     with open(f"{module_config['output_dir']}cached/PL/{ticker}{module_config['timespan_multiplier']}{module_config['timespan']}PL{pid}.json","r") as f:
        #         data = json.loads(f.read())
        #         for _index, similarity in data.items():
        #             index_dict[int(_index)] = similarity

        # for i in range(0, len(indexes)):
        #     _keys = [x for x in indexes]
        #     # n = int(module_config['num_processes']/12)+1
        #     n = int(len(indexes) / 2) + 1
        #
        #     # def scrub_potential_profitable_line_indexes(ticker, ticker_history, i, indexes, module_config):
        #     # _keys = [x for x in data]
        #     # n = batch_size
        #     index_loads = [_keys[i:i + n] for i in range(0, len(_keys), n)]
        #     for _i in range(0, len(index_loads)):
        #         if indexes[i] not in index_loads[_i]:
        #             index_loads[_i].append(indexes[i]) #quick hack for that index bug
        #     loads = [{"test_string":f'{i}/{len(indexes)}', "ticker": ticker, "module_config": module_config, "ticker_history": ticker_history, "i": index_loads[x].index(indexes[i]), "indexes":index_loads[x]} for x in range(0, len(index_loads))]
        #     processes = {}
        #     for load in loads:
        #         p = multiprocessing.Process(target=scrub_potential_profitable_line_indexes, args=(load,))
        #         p.start()
        #         processes[str(p.pid)] = p
        #     pids = [x for x in processes.keys()]
        #     while any(processes[p].is_alive() for p in processes.keys()):
        #         pass
        #         # print(f"Waiting for {len([x for x in processes if x.is_alive()])} processes to complete. Going to sleep for 10 seconds")
        #         # process_str = ','.join([str(v.pid) for v in processes.values() if v.is_alive()])
        #         # print(f"The following child processes are still running: {process_str}")
        #         # time.sleep(0.5)
        #     #ok so once we are here, let's go ahead and load the index dict
        #     for pid in pids:
        #         _pids.append(pid)
        #         # with open(f"{module_config['output_dir']}cached/PL/{ticker}{module_config['timespan_multiplier']}{module_config['timespan']}PL{pid}.json","r") as f:
        #         #     data = json.loads(f.read())
        #         #     for _index, similarity in data.items():
        #         #         index_dict[int(_index)] = similarity
        # for pid in _pids:
        #     # _pids.append(pid)
        #     with open(f"{module_config['output_dir']}cached/PL/{ticker}{module_config['timespan_multiplier']}{module_config['timespan']}PL{pid}.json","r") as f:
        #         data = json.loads(f.read())
        #         for _index, similarity in data.items():
        #             index_dict[int(_index)] = similarity

            # # process_list_concurrently(indexes, scrub_potential_profitable_line_indexes)
            # if indexes[i] not in index_dict: #first pass on the indexes to get n
            #     index_dict[indexes[i]] ={}
            # for ii in range(0, len(indexes)): #second pass on indexes to get comparison
            #     if i == ii:
            #         index_dict[indexes[ii]]=round(1.00,2)
            #
            #     ticker_history_a, ticker_history_b = normalize_prices(ticker_history[indexes[i]-module_config['line_profit_backward_range']:indexes[i]+1],ticker_history[indexes[ii]-module_config['line_profit_backward_range']:indexes[ii]+1] , module_config)
            #     # print(f"Testing {i}/{len(indexes)} ${ticker}:{ticker_history[indexes[i]].dt}:last price ${ticker_history[indexes[i]].close}: => {ii}/{len(indexes)}  ${ticker_history[indexes[ii]].dt}:last price ${ticker_history[indexes[i]].close}: {similarity}")
            #     similarity = determine_line_similarity( ticker_history_a, ticker_history_b, module_config)
            #     index_dict[indexes[ii]]=round(similarity,2)
            #     print(f"Testing {i}/{len(indexes)} ${ticker}:{ticker_history_a[-1].dt}:Control Date : {ticker_history[indexes[i]].dt} => {ii}/{len(indexes)}  ${ticker}:{ticker_history_b[-1].dt}:Control Date: {ticker_history[indexes[ii]].dt}: Similarity: {similarity}")
            # # pass

        cleanup_dict[profit_value]=index_dict
        pass
    # ok so once we get HERE, let's go ahead and clean our indexes
    #make this recursive
    valid = True

    for profit, index_dict in cleanup_dict.items():
        for index, similarity in index_dict.items():
            # for tested_index, similarity in results.items():
            if similarity < module_config['line_similarity_gt']:
                valid = False
                #do cleanup
                if index in potential_profitable_lines[profit]:
                    potential_profitable_lines[profit].remove(index)
    potential_profitable_lines = {k: v for k, v in potential_profitable_lines.items() if len(v) > 8}
    if not valid:
        return scrub_potential_profitable_lines(ticker, ticker_history, potential_profitable_lines, module_config)
    else:
        return potential_profitable_lines
def find_ticker_profitable_lines(ticker, ticker_history,  module_config):
    #ok so the idea here is to re-use some of the reverse loookback logic in the backtester to interate through the ticker history
    #to within the line_profit_backward_range to then determine the profitability at the forward range
    if len(ticker_history) < module_config['line_profit_backward_range']:
        raise Exception(f"Cannot backtest ticker with {len(ticker_history)} history records, need at least {module_config['line_profit_backward_range']}")
    _module_config = module_config
    _module_config['shape_bars']=module_config['line_profit_backward_range']
    backtest_results = {}
    # start x bars in the past so we can do our forward looking
    potential_profitable_lines = {}
    # normalized_history = normalize_prices(ticker_history, ticker_history, module_config, to_five=True)
    for i in reversed(range(_module_config['line_profit_forward_range']+1, len(ticker_history)-_module_config['line_profit_backward_range'])):

        new_i = i * -1 #dumb lol
        if new_i+1+_module_config['line_profit_forward_range'] == 0:
            break

        # profitability = determine_line_profitability(ticker, ticker_history, new_i, _module_config)

        normalized_history =normalize_prices(ticker_history[:new_i+1+_module_config['line_profit_forward_range']], ticker_history[:new_i+1+_module_config['line_profit_forward_range']], module_config, to_five=True)
        profitability =  determine_line_profitability(ticker,normalized_history, -1*(1+_module_config['line_profit_forward_range']), _module_config)
        # tmp_profitability =  determine_line_profitability(ticker,ticker_history, new_i, _module_config)
        # for i in range()
        # print(f"On ${ticker}: Last ({ticker_history[new_i].dt}): ${ticker_history[new_i].close}, Profit Percentage after {module_config['line_profit_forward_range']} bars: {tmp_profitability} (${ticker_history[new_i+module_config['line_profit_forward_range']].close})| Normalized to $5.00: {normalized_history[-1-_module_config['line_profit_forward_range']].close},  Profit Percentage after {module_config['line_profit_forward_range']}: {profitability} (${normalized_history[-1].close})")
        # print(f"On ${ticker}: Last ({normalized_history[new_i].dt}): ${normalized_history[new_i].close}, Profit Percentage after {module_config['line_profit_forward_range']} bars: {profitability} (${normalized_history[new_i+module_config['line_profit_forward_range']].close})| Normalized to $5.00: {normalized_history[new_i].close},  Profit Percentage after {module_config['line_profit_forward_range']}: {profitability} (${new_normal[new_i+module_config['line_profit_forward_range']].close})")
        # print(f"Normalized to $5.00: {normalized_history[new].close},  Profit Percentage after {module_config['line_profit_forward_range']}: {profitability} (${normalized_history[i+module_config['line_profit_forward_range']].close})")
        if profitability  >= _module_config['line_profit_minimum'] or profitability <= _module_config['line_profit_minimum']*-1:
            #todo write the profitz hurr
            # print(f"Found profitable line on {ticker}:{human_readable_datetime(timestamp_to_datetime(ticker_history[i].timestamp))}: Profit Percentage: {profitability}")
            if round(float(profitability),2) not in potential_profitable_lines:

                potential_profitable_lines[round(float(profitability),2)] = []
            potential_profitable_lines[round(float(profitability),2)].append(new_i)

    #blah ok now that i'm not sleepy let's try to figure this out
    #basically we need to compare each potential profitable line to the other profitable lines we've found so far
    #if they're the same line, we can keep them in the listing, otherwise we need to remove them

    # profitable_lines = scrub_potential_profitable_lines(ticker, ticker_history,{[x for x in potential_profitable_lines.keys()][0]:potential_profitable_lines[[x for x in potential_profitable_lines.keys()][0]]}, module_config)
    potential_profitable_lines = {k:v for k,v in potential_profitable_lines.items() if len(v) > 1}

    profitable_lines = scrub_potential_profitable_lines(ticker, ticker_history, potential_profitable_lines, module_config)
    return profitable_lines

    # pass
def determine_line_profitability(ticker, ticker_history, index, module_config):
    '''
    Use this functino to determine the profitability of a line, pass in a starting index and a forward range,the forward range
    being the range we compare to the index to determine profit. Profitability is calculated as a percentage of the underlying
    This function requires the following module configs:

    line_profit_minimum- the profit percentage (+/-) we need to exceed to write this to the DB as a profitable line
    line_profit_forward_range
    line_profit_backward_range
    :param ticker:
    :param ticker_history:
    :param forward_range:
    :param index:
    :param module_config:
    :return:
    '''
    # pass
    # return 1
    #ok so the idea here is to compare the history[index] to hisory[index+forward_range] and calculate the percentage
    # not sure why this isn't working

    if ticker_history[index + module_config['line_profit_forward_range']].close >= ticker_history[index].close:
        #ifthe move is positive
        #find the change
        delta = ticker_history[index + module_config['line_profit_forward_range']].close - ticker_history[index].close
        multiplier = 1

    else:
        #the move is negative
        delta = ticker_history[index].close -  ticker_history[index + module_config['line_profit_forward_range']].close
        multiplier = -1

    return round(float(calculate_x_is_what_percentage_of_y(delta, ticker_history[index].close)  *float(multiplier)),2)
    # return round(calculate_x_is_what_percentage_of_y(ticker_history[index+module_config['line_profit_forward_range']].close-ticker_history[index].close, ticker_history[index].close ) * 1.00 if ticker_history[index+module_config['line_profit_forward_range']].close >= ticker_history[index].close else -1.00, 2)


# def determine_line_similarity(_x, _y):
#
#     x = np.linspace(1, -1, num=200)
#
#     y1 = 2*x**2 + 1
#     y2 = 2*x**2 + 2
#
#     shape1 = np.column_stack((x, y1))
#     shape2 = np.column_stack((x, y2))
#
#     similarity = shape_similarity(shape1, shape2,checkRotation=False)
#
#     # plt.plot(shape1[:,0], shape1[:,1], linewidth=2.0)
#     # plt.plot(shape2[:,0], shape2[:,1], linewidth=2.0)
#     #
#     # plt.title(f'Shape similarity is: {similarity}', fontsize=14, fontweight='bold')
#     # plt.show()


def adjust_ticker_history_by_percentage(ticker_history,percentage, module_config):
    th = []
    for i in range(0, len(ticker_history)):
        pass
        close_one_percent = ticker_history[i].close/100
        open_one_percent = ticker_history[i].open/100
        high_one_percent = ticker_history[i].high/100
        low_one_percent = ticker_history[i].low/100
        # float(percentage * one_percent)
        # def __init__(self, open, close, high, low, volume, timestamp):
        #
        th.append(TickerHistory(round(float(percentage*open_one_percent),2),round(float(percentage*close_one_percent),2),round(float(percentage*high_one_percent),2),round(float(percentage*low_one_percent),2), ticker_history[i].volume, ticker_history[i].timestamp))
    return th
def normalize_prices(ticker_history_a, ticker_history_b, module_config, to_five=False):
    # ok first we detemine the larger of the two
    if to_five: #normalize to $5, or a penny stock cutoff
        # if ticker_history_a[-1].close > 5.00:
        # we need to figure out what percentage of a  is b
        # percentage = int(float((ticker_history_a[-1].close - 5) / 5.00) * 100)*-1
        # percentage = int(5.00/float(ticker_history_a[-1].close ) * 100) /100
        # one_percent = ticker_history_b[-1].close / 100
        new_ticker_history =[]
        percentage = int(float((ticker_history_a[-1].close - 5) / 5.00) * 100)
        for hist  in ticker_history_a:
            new_ticker_history.append(TickerHistory(hist.open - ((5.00 / 100) * percentage),hist.close - ((5.00 / 100) * percentage),hist.high - ((5.00 / 100) * percentage),hist.low - ((5.00 / 100) * percentage), hist.volume, hist.timestamp))
        return new_ticker_history
        # ok so first
        # pass
        # else:
        #     new_ticker_history = []
        #     percentage = int(float((ticker_history_a[-1].close - 5) / 5.00) * 100)
        #     for hist in ticker_history_a:
        #         new_ticker_history.append(
        #             TickerHistory(hist.open - ((5.00 / 100) * percentage), hist.close - ((5.00 / 100) * percentage),
        #                           hist.high - ((5.00 / 100) * percentage), hist.low - ((5.00 / 100) * percentage),
        #                           hist.volume, hist.timestamp))
        #     return new_ticker_history

    if ticker_history_a[-1].close > ticker_history_b[-1].close:
        # we need to figure out what percentage of a  is b
        percentage = int(float(ticker_history_a[-1].close / ticker_history_b[-1].close)*100)
        one_percent = ticker_history_b[-1].close/100
        return ticker_history_a,adjust_ticker_history_by_percentage(ticker_history_b, percentage, module_config)
        #ok so first
        pass
    else:
        percentage = int(float(ticker_history_b[-1].close / ticker_history_a[-1].close)*100)
        # one_percent = ticker_history_a[-1].close/100
        return adjust_ticker_history_by_percentage(ticker_history_a, percentage, module_config),ticker_history_b
