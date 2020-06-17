# fundamental modules
import glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import datetime
from utils import write_to_db_update, query_to_db
import ib_insync
from ib_insync import *
util.startLoop()  # uncomment this line when in a notebook


# get last date from database
last_datetime = query_to_db('SELECT MAX(date) AS max_date FROM VIX;', "odvjet12_market_data_usa")

# ib connection
ib = IB()
ib.connect('127.0.0.1', 7496, clientId=1)

# get minute data for every contract
contract = Index(symbol='VIX', exchange='CBOE', localSymbol='VIX', currency='USD')
try:
    bars = ib.reqHistoricalData(
        contract,
        endDateTime=datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'),
        durationStr='5 D',
        barSizeSetting='1 min',
        whatToShow='TRADES',
        useRTH=True,
        formatDate=1
    )
    df = util.df(bars)
except TypeError as te:
    print(te)

# clean df
df.set_index(df.date, inplace=True)
df.drop(columns=['date'], inplace=True)
df.sort_index(inplace=True)
df.index = df.index.tz_localize('utc').tz_convert('America/Chicago').strftime("%Y-%m-%d %H:%M:%S")  #convert timezones to vix timzone
df['date'] = pd.to_datetime(df.index)
df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'average', 'barCount']]
df = df.iloc[:-1,]

# take only new
vix_new = df[last_datetime.astype(str).iloc[0,0]:]
vix_merged = pd.concat([df, vix_new], axis=0)

# add to db table
write_to_db_update(vix_merged, "odvjet12_market_data_usa", 'VIX')

# disconnect interactive brokers
ib.disconnect()
