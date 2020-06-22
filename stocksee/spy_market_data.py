# fundamental modules
import glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import datetime
from utils import write_to_db
import ib_insync
from ib_insync import *
util.startLoop()  # uncomment this line when in a notebook



### GLOBAL (CONFIGS)
SPY_DATA_PATH = 'C:/Users/Mislav/algoAItrader/data/'


### PANDAS OPTIONS
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


#### IMPORT RAW SPY DATA, REMOVE NA VALUES AND MERGE WITH IB SPY DATA

# import raw SPY data
spyRaw = pd.read_csv(
    SPY_DATA_PATH + 'SpyVixWithIndicators.csv', sep=';', decimal=',',
    usecols=list(range(59)),parse_dates=['TimeBarStart'], 
    index_col='TimeBarStart'
    )

# keep OHLC data, remove other
spy = spyRaw[['SpyFirstTradePrice', 'SpyHighTradePrice', 'SpyLowTradePrice',
            'SpyLastTradePrice', 'SpyVolume']]
spy.columns = ['open', 'high', 'low', 'close', 'volume']

# missing data spy
print(spy.isna().sum())
spy = spy.dropna(subset=['close'])
print(spy.isna().sum())

# prepare VIX for database importing
spy['date'] = spy.index
spy = spy[['date', 'open', 'high', 'low', 'close', 'volume']]


### ADD VIX DATA FROM INTERACTIVE BROKERS
# connection
ib = IB()
ib.connect('127.0.0.1', 7496, clientId=1)

# get minute data for every contract
contract = Stock('SPY', 'SMART', 'USD')
date_end_loop = pd.Timestamp('2000-12-31 23:59:59')
max_date = datetime.datetime.now()
data = []
while max_date > date_end_loop:
    max_date = max_date.strftime('%Y%m%d %H:%M:%S')
    try:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=max_date,
            durationStr='5 M',
            barSizeSetting='1 min',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1
        )
        df = util.df(bars)
        max_date = df['date'].min()
        data.append(df)
    except TypeError as te:
        print(te)
        break
    
    
# merge all df's
spy_df = pd.concat(data, axis=0)
spy_df.set_index(spy_df.date, inplace=True)
spy_df.drop(columns=['date'], inplace=True)
spy_df.sort_index(inplace=True)
spy_df.index = spy_df.index.tz_localize('utc').tz_convert('America/Chicago').strftime("%Y-%m-%d %H:%M:%S")  #convert timezones to vix timzone
spy_df['date'] = pd.to_datetime(spy_df.index)
spy_df = spy_df[['date', 'open', 'high', 'low', 'close', 'volume', 'average', 'barCount']]

# merge vix dbs
spy_new = spy_df['2018-12-31 19:59:00':]
spy_merged = pd.concat([spy, spy_new], axis=0)
spy_merged.head()
spy_merged.tail()

# remove duplicates
print(spy_merged.shape)
spy_merged.drop_duplicates(keep='first', inplace=True)
print(spy_merged.shape)

# add to database
write_to_db(spy_merged, "odvjet12_market_data_usa", 'SPY')

# disconnect interactive brokers
ib.disconnect()
