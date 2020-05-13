import os
import glob
import shutil
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
import datetime
import time
import json
from utils import query_to_db
from utils import first_column_to_names
from utils import write_to_db
import ib_insync
from ib_insync import *
util.startLoop()  # uncomment this line when in a notebook


# pandas options
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# connection
ib = IB()   
ib.connect('127.0.0.1', 7496, clientId=1)

# contracts
con_list = ['AAPL', 'AMZN', 'GOOG', 'FB', 'PG', 'WMT', 'V']
contracts = [Stock(i, 'SMART', 'USD') for i in pd.Series(con_list)]

i = 5
# get minute data for every contract
date_end_loop = pd.Timestamp('2003-12-31 23:59:59')
max_date = datetime.datetime.now()
os.mkdir(con_list[i])
save_path_prefix = con_list[i] + '/ohlc_' + con_list[0] + '_'
while max_date > date_end_loop:
    max_date = max_date.strftime('%Y%m%d %H:%M:%S')
    try:
        bars = ib.reqHistoricalData(
            contracts[i],
            endDateTime=max_date,
            durationStr='6 M',
            barSizeSetting='1 min',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1
        )
        df = util.df(bars)
        max_date = df['date'].min()
        df.to_csv(save_path_prefix + max_date.strftime('%Y-%m-%d') + '.csv',
                index=False, sep=';')
    except TypeError as te:
        print(te)
        break


# clean scraped tables
files = glob.glob(save_path_prefix + '*')
market_data = [pd.read_csv(f, sep=';', parse_dates=['date']) for f in files]
market_data = pd.concat(market_data, axis=0)
market_data = market_data.merge(pd.Series(con_list[i]).rename('ticker'), how='left',
                                left_index=True, right_index=True)
market_data.drop_duplicates(inplace=True)
market_data.sort_index(inplace=True)

# save final table to db and hdf5 file
write_to_db(market_data, "odvjet12_market_data_usa", con_list[i])
store_path = 'D:/market_data/usa/' + con_list[i] + '.h5'
market_data.to_hdf(store_path, key=con_list[i])

# delete csv files
shutil.rmtree(con_list[i])

# disconnect interactive brokers
ib.disconnect()
