import pandas as pd
import numpy as np
import janitor
import datetime
import string
from utils import requests_retry_session
from utils import get_request
from utils import write_to_db
from utils import write_to_db_update
from utils import delete_rows
from utils import query_to_db
import ib_insync
from ib_insync import *
util.startLoop()  # uncomment this line when in a notebook


if __name__ == '__main__':

    ### HELP FUNCTIONS

    def get_table(res):
        stocks = pd.read_html(res.text, attrs={"class": "quotes"}, header=0,
                            thousands=",", decimal='.')[0]
        stocks = janitor.clean_names(stocks)
        return stocks


    def get_tickers(urls, exchange):
        res = [get_request(url) for url in urls]
        tickers = [get_table(r) for r in res]
        tickers = pd.concat(tickers, axis=0)
        tickers['exchange'] = exchange
        return tickers


    ### SCRAP TICKER FROM http://www.eoddata.com/default.aspx

    # urls
    urls = {
        'amex': [f'http://www.eoddata.com/stocklist/AMEX/{l}.htm' for l in string.ascii_uppercase],
        'nasdaq': [f'http://www.eoddata.com/stocklist/NASDAQ/{l}.htm' for l in string.ascii_uppercase],
        'nyse': [f'http://www.eoddata.com/stocklist/NYSE/{l}.htm' for l in string.ascii_uppercase]
    }

    # loop to obtain data from every url
    tickers = [get_tickers(value, key) for key, value in urls.items()]
    tickers_df = pd.concat(tickers, axis=0)
    tickers_df = tickers_df[['code', 'name', 'high', 'low', 'close', 'volume', 'exchange']]
    tickers_df['date'] = datetime.date.today()

    # add to database
    write_to_db(tickers_df, "odvjet12_stocks", "stocks_usa")
    
    # connection
    ib = IB()
    ib.connect('127.0.0.1', 7497, clientId=1)

    # get tickers THIS STEP CAN BE REMOVED
    tickers = query_to_db('SELECT DISTINCT code FROM stocks_usa', 'odvjet12_stocks')
    tickers = tickers.code.to_list()

    # get contract details
    contracts = [ib.reqContractDetails(Stock(tick, 'SMART', 'USD')) for tick in tickers]
    contracts_short = [con for con in contracts if len(con) > 0]
    contract_types = [con[0].stockType for con in contracts_short]
    pd.Series(contract_types).value_counts()  # security types

    # extract contract column and other columns
    contract_column = util.df([con[0].contract for con in contracts_short])
    rest_columns = util.df([con[0] for con in contracts_short])
    rest_columns = rest_columns.iloc[: , 1:]
    contracts_details = pd.concat([contract_column, rest_columns], axis=1)

    # for now I will delete comboLegs column and secIdList columnd
    #  because they are lists. If I will need them try to unnest
    contracts_details.drop(columns=['comboLegs', 'secIdList'], inplace=True)
    
    # add to database
    write_to_db(contracts_details, "odvjet12_stocks", "stocks_usa_details")
