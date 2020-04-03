from utils import requests_retry_session
from utils import get_request
from utils import first_column_to_names
from utils import rbind
from utils import str_to_float
from utils import write_to_db
import re
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import janitor
import pymysql


if __name__ == '__main__':
    # scrap stocks from: https://dev.to/ssbozy/python-requests-with-retries-4p03
    res = get_request("https://zse.hr/default.aspx?id=26486")
    stocks = pd.read_html(res.text, attrs={"class": "dnevna_trgovanja"}, header=1,
                        parse_dates=['Listing date'], thousands=".")[0]
    stocks = janitor.clean_names(stocks)

    # scrap additional info for every stock
    bs = BeautifulSoup(res.text, "html.parser")
    links = bs.find_all(href=True)
    links = [tag['href'] for tag in links]
    links = [link for link in links if re.search(r"^\?id.*dionica=.*", link)]
    links = ["https://zse.hr/default.aspx" + link for link in links]
    stockDetails = []
    for link in links:
        print(link)
        r = get_request(link)
        bs = BeautifulSoup(r.text, 'html.parser')
        sector_code = bs.select('span.toolTip a')[0].get_text()
        sector_code = pd.DataFrame({'nacerev': [sector_code]})
        try:
            tradingData = pd.read_html(r.text, attrs={"class": "dioniceSheet1"}, 
                                    match="52WK", thousands=".", decimal=",")[0]
            tradingData = first_column_to_names(tradingData)
            tradingData = tradingData[['last', '52wk_high', '52wk_low', 
                                    'best_buy_bid', 'best_sell_bid']]
        except:
            tradingData = None
        category = pd.read_html(r.text, attrs={"class": "dioniceSheet1"},
                                match="Category", thousands=".", decimal=",")[0]
        category = first_column_to_names(category)
        details = pd.read_html(r.text, attrs={"class": "dioniceSheet1"},
                            match="Nominal")[0]
        details = first_column_to_names(details)
        try:
            market_makers = pd.read_html(r.text, attrs={"class": "Name"}, 
                                        match="Nominal")[0]
            market_makers = first_column_to_names(market_makers)
        except ValueError:
            market_makers = None
        listed = bs.select('div#t17561 div.dionica_content p')
        if listed:
            listed = 1
        else:
            listed = 0 
        listed = pd.DataFrame({'listed': listed}, index=[0])
        allTables = rbind([sector_code, tradingData, category, details, 
                        market_makers, listed])
        stockDetails.append(allTables)

    # rbind all elements
    zseStocks = pd.concat(stockDetails, axis=0, sort=False)
    zseStocks = pd.concat([stocks.reset_index(drop=True), 
                        zseStocks.reset_index(drop=True)], axis=1)
    zseStocks = zseStocks.iloc[:, ~zseStocks.columns.duplicated()]

    # clean final table (change column names, dta types, add ew scalar columns)
    deleteCols = ['par_value', 'sector_', 'security_prospectus', 
                'top_10_accounts_per_security']
    numericCols = ['last', '52wk_high', '52wk_low','best_buy_bid', 
                'best_sell_bid', 'category', 'liquidity_band', 'nav']
    strToNumeric = ['cash_dividend', 'nominal_value', 'discount']
    dateCols = ['ex_dividend_date', 'payment_date', 'record_date', 'nav_date']
    zseStocks[numericCols] = zseStocks[numericCols].apply(pd.to_numeric, 
                                                        errors='coerce', axis=1)
    zseStocks[dateCols] = zseStocks[dateCols].apply(pd.to_datetime, 
                                                    errors='coerce', axis=1)
    zseStocks = zseStocks.drop(columns=deleteCols)
    zseStocks[strToNumeric] = zseStocks[strToNumeric].applymap(str_to_float)
    zseStocks['discount'] = zseStocks['discount'].divide(100)
    zseStocks['exchange'] = "zse"
    zseStocks['currency'] = "HRK"
    zseStocks['market_capitalization'] = zseStocks['shares'].mul(zseStocks['last'])
    zseStocks = zseStocks.rename(columns={
        'ticker': 'symbol',
        'issuer': "company_name",
        'nacerev': 'sector_code',
        '52wk_high': 'price_week52_high',
        '52wk_low': 'price_week52_low',
        'security_type': 'type',
        'shares': "number_shares_outstanding",
        'cash_dividend': 'dividend',
        'payment_date': 'dividend_payment_date',
        'record_date': 'dividend_record_date'
    })

    # add to database
    write_to_db(zseStocks, "odvjet12_stocks", "zse_stocks")
