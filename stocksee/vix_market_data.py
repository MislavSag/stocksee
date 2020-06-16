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

# vix
vix_columns = ['VixFirstTradePrice', 'VixHighTradePrice', 
                'VixLowTradePrice', 'VixLastTradePrice', 'VixVolume']
vix = spyRaw[vix_columns]
vix.columns = ['vixFirst', 'vixHigh', 'vixLow', 'vixClose', 'vixVolume']

# missing data vix
print("Missing vlaues by cols \n", vix.isna().sum())
print("Share of missing values in spy \n", vix.isna().sum().iloc[0] / spyRaw.shape[0])  # NA share in spy
isnaTime = vix.isna().any(1).index.date
isnaTime = (pd.DataFrame(isnaTime, columns=['time']).
            groupby(['time']).size().sort_values(ascending=False))
print(isnaTime.head(10))
vix.loc['1998-01-21 09:00:00':'1998-01-21 12:00:00']
vix.dropna(inplace=True)  # remove Nan values

# prepare VIX for database importing
vix['date'] = vix.index
vix = vix[['date', 'vixFirst', 'vixHigh', 'vixLow', 'vixClose', 'vixVolume']]
vix.columns = ['date', 'open', 'high', 'low', 'close', 'volume']


### ADD VIX DATA FROM INTERACTIVE BROKERS
# connection
ib = IB()
ib.connect('127.0.0.1', 7496, clientId=1)

# get minute data for every contract
contract = Index(symbol='VIX', exchange='CBOE', localSymbol='VIX', currency='USD')
date_end_loop = pd.Timestamp('2003-12-31 23:59:59')
max_date = datetime.datetime.now()
# os.mkdir('VIX')
# save_path_prefix = 'market_data' + '/ohlc_' + 'VIX' + '_'
data = []
while max_date > date_end_loop:
    max_date = max_date.strftime('%Y%m%d %H:%M:%S')
    try:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=max_date,
            durationStr='6 M',
            barSizeSetting='1 min',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1
        )
        df = util.df(bars)
        max_date = df['date'].min()
        data.append(df)
        # df.to_csv(save_path_prefix + max_date.strftime('%Y-%m-%d') + '.csv',
        #         index=False, sep=';')
    except TypeError as te:
        print(te)
        break

# merge all df's
vix_df = pd.concat(data, axis=0)
vix_df.set_index(vix_df.date, inplace=True)
vix_df.drop(columns=['date'], inplace=True)
vix_df.sort_index(inplace=True)
vix_df.index = vix_df.index.tz_localize('utc').tz_convert('America/Chicago')  #convert timezones to vix timzone
vix_df['date'] = pd.to_datetime(vix_df.index).strftime("%Y-%m-%d %H:%M:%S")
vix_df = vix_df[['date', 'open', 'high', 'low', 'close', 'volume', 'average', 'barCount']]
vix_df.head()
vix_df.tail()

# merge vix dbs
vix_new = vix_df['2019-03-30 00:00:00':]
vix_merged = pd.concat([vix, vix_new], axis=0)
vix_merged.head()
vix_merged.tail()

# add to database
write_to_db(vix, "odvjet12_market_data_usa", 'VIX')

# disconnect interactive brokers
ib.disconnect()
