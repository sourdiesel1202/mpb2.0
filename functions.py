import datetime
import itertools
import json, csv, os
import multiprocessing
import time
import traceback
from zoneinfo import ZoneInfo

import pymysql


def load_module_config(module):
    print(f"Loading config file for {module}")
    with open(f"configs/{module}.json", "r") as f:
        module_config =  json.loads(f.read())
    with open(f"configs/database.json", "r") as f:
        module_config['database'] = {}
        for k, v in json.loads(f.read()).items():
            module_config['database'][k]=v
    with open(f"configs/strategy.json", "r") as f:
        module_config['strategy_configs'] = {}
        for k, v in json.loads(f.read()).items():
            module_config['strategy_configs'][k]=v

    with open(f"configs/indicators.json", "r") as f:
        module_config['indicator_configs'] = {}
        for k, v in json.loads(f.read()).items():
            module_config['indicator_configs'][k]=v
    print(json.dumps(module_config))
    return  module_config
def write_csv(filename, rows):
    with open(filename  , 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    # print(f"output file written to {filename}")
    # global_workbook.sheets.append('reports/sheets/'+filename)
def read_csv(filename):
    result = []
    with open(filename,'r', newline='', encoding='utf-8') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')

        for row in spamreader:
            result.append([x for x in row])
    return  result
def delete_csv(filename):
    os.system(f"rm {filename}")
    # print(f"Deleted file {filename}")

def generate_csv_string(rows):
    filename = f"tmp{datetime.datetime.now().month}{datetime.datetime.now().day}{datetime.datetime.now().year}{datetime.datetime.now().minute}{datetime.datetime.now().second}{os.getpid()}.csv"
    write_csv(filename,rows)
    res = ""
    with open(filename, "r") as f:
        res = f.read()
    delete_csv(filename)
    return res

def combine_jsons(files):
    results = []
    for _file in files:
        with open(_file, "r") as f:

            for entry in json.loads(f.read()):
                results.append(entry)
        os.system(f"rm {_file}")
    return results
def combine_csvs(files):
    _filestr = '\n'.join(files)
    # print(f"Combining the following files {_filestr}")
    records = []
    for _file in files:
        rows= read_csv(_file)
        if len(records) == 0:
            records.append(rows[0])

        for i in range(1, len(rows)):
            records.append(rows[i])
        delete_csv(_file)
    return records
def get_today(module_config, minus_days=0):

    if module_config['test_mode']:
        d = datetime.datetime.strptime(module_config['test_date'], "%Y-%m-%d")
    else:
        d=  datetime.datetime.now()
    if minus_days > 0:
        return (d - datetime.timedelta(days=minus_days)).strftime("%Y-%m-%d")
    else:
        return d.strftime("%Y-%m-%d")
def calculate_x_is_what_percentage_of_y(x, y):
    try:
        return (float(x)/float(y))*100.00
    except:
        return 0.0

def calculate_what_is_x_percentage_of_y(x, y):
    try:
        return float(x)/100.00*float(y)
    except:
        return 0.0


def timestamp_to_datetime(timestamp):
    return datetime.datetime.fromtimestamp(float(timestamp) / 1e3, tz=ZoneInfo('US/Eastern'))#.strftime("%Y-%m-%d %H:%M:%S")

def datetime_to_timestamp():
    pass

def human_readable_datetime(_d):
    return _d.strftime("%Y-%m-%d %H:%M:%S")
def process_list_concurrently(data, process_function, batch_size):
    '''
    Process a list concurrently
    :param data: the list to process
    :param process_function: the function to pass to the multiprocessing module
    :param batch_size: the number of records to process at a time
    :return: None
    '''
    _keys = [x for x in data]
    n = batch_size
    loads = [_keys[i:i + n] for i in range(0, len(_keys), n)]
    # for load in loads:
    #     load.insert(0, data[0])
    # for load in loads:
    #     print(f"Load size: {len(load)}")
    # return
    processes = {}
    for load in loads:
        # p = multiprocessing.Process(target=process_function, args=(load,))
        p = multiprocessing.Process(target=process_function, args=(load,))
        p.start()

        processes[str(p.pid)] = p
    pids = [x for x in processes.keys()]
    while any(processes[p].is_alive() for p in processes.keys()):
        # print(f"Waiting for {len([x for x in processes if x.is_alive()])} processes to complete. Going to sleep for 10 seconds")
        process_str = ','.join([str(v.pid) for v in processes.values() if v.is_alive()])
        print(f"The following child processes are still running: {process_str}")
        time.sleep(10)
    return pids


def obtain_db_connection(module_config):
    pass
    return pymysql.connect(
        host=module_config['database']['host'],
        user=module_config['database']['username'],
        password=module_config['database']['password'],
        db=module_config['database']['db'],
    )
    # if 'service' in creds[env].keys():
    #     connection = cx_Oracle.connect(creds[env]['username'], decrypt_message(creds[env]['password']),
    #                                    cx_Oracle.makedsn(creds[env]['host'], creds[env]['port'],
    #                                                      service_name=creds[env]['service']))
    # else:
    #     connection = cx_Oracle.connect(creds[env]['username'], creds[env]['password'],
    #                                    cx_Oracle.makedsn(creds[env]['host'], creds[env]['port'],
    #                                                      sid=creds[env]['sid']))
    # print(f"connection obtained to {env}: {creds[env]['host']}")
    # return connection

def execute_query(connection,sql, verbose=False):
    cursor = connection.cursor()
    try:
        if verbose:
            print(f"Executing\n{sql}")
        cursor.execute(sql)
        result= []
        result.append([row[0] for row in cursor.description])
        for row in cursor.fetchall():
            result.append([str(x) for x in row])
        if verbose:
            print(f"{len(result)-1} rows returned")
        cursor.close()
        return result

    except:
        cursor.close()
        traceback.print_exc()

def execute_update(connection,sql, auto_commit=True, verbose=False, cache=True):
    if verbose:
        print(sql)
    cursor = connection.cursor()
    try:
        # pass
        if cache:
            with open(f"sql/{os.getpid()}_updates.sql", "a+") as f:
                f.writelines(f"{sql}\n")
                pass
        else:
            try:
                cursor.execute(sql)
            except Exception as e:
                # traceback.print_exc()
                with open(f"sql/errors.sql", "a+") as f:
                    f.writelines(f"{sql}")
                    pass

        if auto_commit:
            try:
                connection.commit()
            except:
                traceback.print_exc()
            connection.commit()
        cursor.close()
        pass
    except:
        cursor.close()
        traceback.print_exc()
def get_permutations(lists):
    unique_combinations = []

    # Getting all permutations of list_1
    # with length of list_2
    # permut =

    # zip() is called to pair each permutation
    # and shorter list element into combination
    # for comb in permut:
        # zipped = zip(comb, list_2)
        # unique_combinations.append(list(zipped))
    return  list(itertools.product(*lists))
def all_possible_combinations(list_of_stuff):
    combos=[]
    for L in range(len(list_of_stuff) + 1):
        for subset in itertools.combinations(list_of_stuff, L):
            combos.append(list(subset))
    return combos