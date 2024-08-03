import math
import time
import traceback

from shapely import LineString, frechet_distance
from shapesimilarity import shape_similarity, procrustes_normalize_curve, curve_length
# import matplotlib.pyplot as plt
import numpy as np
# from statistics import mean

# from db_functions import load_ticker_symbol_by_id, load_ticker_history_by_id
# from db_functions import load_profitable_line_matrix
from functions import timestamp_to_datetime, human_readable_datetime, execute_query, execute_update
# from functions import calculate_x_is_what_percentage_of_y
# from history import load_ticker_history_db



def compare_tickers(ticker_a, ticker_history_a,ticker_b, ticker_history_b, module_config):
    return determine_line_similarity(ticker_history_a,ticker_history_b,module_config)
    # return determine_line_similarity(**format_ticker_data(ticker_a, ticker_history_a,ticker_b, ticker_history_b, module_config))

def compare_tickers_at_index(index, ticker_a, ticker_history_a,ticker_b, ticker_history_b, module_config):
    #ok just so we're clear on the idea here
    #basically just call compare_tickers but pre-filter the ticker history
    #and stub in the shape_bars for line_profit_backward_range
    _module_config = module_config
    _module_config['shape_bars'] = module_config['line_profit_backward_range']
    return compare_tickers(ticker_a, ticker_history_a, ticker_b, ticker_history_b[:index+1], module_config)




def format_ticker_data(ticker_a, ticker_history_a,ticker_b, ticker_history_b, module_config, ignore_timestamps=False):
    '''
    Ok so the idea here is that we need to format the ticker data in a way that is in line with the other ticker
    :param ticker:
    :param ticker_history:
    :param module_config:
    :return:
    '''
    matches= {"ticker_a":[], "ticker_b":[], "timestamps":[]}
    # a_timestamps = [x.timestamp for x in ticker_history_a]
    # b_timestamps = [x.timestamp for x in ticker_history_b]
    if len(ticker_history_a) < module_config['shape_bars'] or len(ticker_history_b) < module_config['shape_bars']:
        raise Exception(f"Cannot calculate shape similarity between {ticker_a} (size: {len(ticker_history_a)}) and {ticker_b} (size: {len(ticker_history_b)}), not enough historical bars for {module_config['shape_bars']} ")

    for i in range(1, module_config['shape_bars']):
        #only really care about n bars in the past, configured in module_config
        if not ignore_timestamps:
            if ticker_history_a[-i].timestamp == ticker_history_b[-i].timestamp:
                matches['ticker_a'].append(ticker_history_a[-i].close)
                matches['ticker_b'].append(ticker_history_b[-i].close)
                matches['timestamps'].append(int(ticker_history_b[-i].timestamp))
            else:
                matches['ticker_a'].append(ticker_history_a[-i].close)
                matches['ticker_b'].append(ticker_history_b[-i].close)
                matches['timestamps'].append(int(ticker_history_a[-i].timestamp))

    return  matches
def determine_line_similarity(ticker_history_a,ticker_history_b, module_config) :


    # # similarity =
    # try:
    #     xs = [i for i in range(0, len(timestamps))]
    #     shape1 = np.column_stack((np.array(xs), np.array(ticker_a)))
    #     shape2 = np.column_stack((np.array(xs), np.array(ticker_b)))
    #     return shape_similarity(shape1, shape2,checkRotation=1)
    # except Exception as e:
    #     raise e
    # pass

    # x = np.linspace(module_config['shape_bars'], 0, num=module_config['shape_bars'])
    # y1 = 2 * x ** 2 + 1
    # y2 = 2 * x ** 2 + 2

    x = np.linspace(1, -1, num=module_config['shape_bars'])
    shape1 = np.column_stack((x, [x.close for x in ticker_history_a[module_config['shape_bars'] * -1:]]))
    shape2 = np.column_stack((x, [x.close for x in ticker_history_b[module_config['shape_bars'] * -1:]]))
    #
    try:
        return shape_similarity(shape1, shape2,checkRotation=False)
    except Exception as e:
        traceback.print_exc()
        raise e
    # y1s = [x.close for x in ticker_history_a[module_config['shape_bars'] * -1:]]
    # xs = [i for i in range(0, module_config['shape_bars'])]
    # y2s = [x.close for x in ticker_history_b[module_config['shape_bars'] * -1:]]
    # ticker_a_line = LineString([[xs[i], y1s[i]] for i in range(0, len(xs))])
    # ticker_b_line = LineString([[xs[i], y2s[i]] for i in range(0, len(xs))])
    # f = frechet_distance(ticker_a_line, ticker_b_line)
    # x = np.linspace(1, -1, num=module_config['shape_bars'])
    # shape1 = np.column_stack((x, [x.close for x in ticker_history_a[module_config['shape_bars'] * -1:]]))
    # shape2 = np.column_stack((x, [x.close for x in ticker_history_b[module_config['shape_bars'] * -1:]]))
    # procrustes_normalized_curve1 = procrustes_normalize_curve(shape1)
    # procrustes_normalized_curve2 = procrustes_normalize_curve(shape2)
    # geo_avg_curve_len = math.sqrt(
    #     curve_length(procrustes_normalized_curve1) *
    #     curve_length(procrustes_normalized_curve2)
    # )
    # return max(1 - f / (geo_avg_curve_len / math.sqrt(2)), 0)

# Testing $AUGX:2023-06-27 09:30:00:DB Value : 2023-06-27 09:30:00-04:00 => $AMPS:2023-06-26 11:30:00:DB Value: 2023-06-26 11:30:00-04:00: 0.9322
# Testing $AUGX:2023-06-27 09:30:00:DB Value : 2023-06-27 09:30:00-04:00 => $EAT:2023-05-31 12:30:00:DB Value: 2023-05-31 12:30:00-04:00: 0.5298
# Testing $AUGX:2023-06-27 09:30:00:DB Value : 2023-06-27 09:30:00-04:00 => $MSGM:2023-05-10 15:30:00:DB Value: 2023-05-10 15:30:00-04:00: 0.94
# Testing $AUGX:2023-06-27 09:30:00:DB Value : 2023-06-27 09:30:00-04:00 => $KRT:2023-05-10 14:30:00:DB Value: 2023-05-10 14:30:00-04:00: 0.8058