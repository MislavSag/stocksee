from utils import requests_retry_session
from utils import get_request
from utils import write_to_db
import pandas as pd
import janitor


if __name__ == '__main__':
    # dates to scrap trade data for
    dateList = pd.date_range("1994-01-01", pd.Timestamp.today())

    # get trade dat for every date
    zseList=[]
    for datei in dateList:
        
        # post request
        formData = {
            "selDatum": datei,
            "btnSave": "Show",
            "IsItPregledPovijest": "yes",
            "rbScope": "svi"
        }
        response = requests_retry_session().post(
            'https://zse.hr/default.aspx?id=26523', data=formData
            )
        tblByDate = pd.read_html(response.text, attrs={'id': 'dnevna_trgovanja'}, 
                                thousands=".", decimal=',')[0]
        if len(tblByDate.index) == 0:
            continue
    
        # clean table
        tblByDate = janitor.clean_names(tblByDate)
        tblByDate = tblByDate.rename(columns={
            'ticker': 'symbol', 'change_%': 'change'})
        tblByDate['change'] = tblByDate['change'].str.extract('(.\d+,\d+)')
        tblByDate['change'] = tblByDate['change'].str.replace(',', '.')
        tblByDate.loc[:, 'close':'turnover'] = tblByDate.loc[:, 'close':'turnover']\
            .applymap(lambda x: pd.to_numeric(x, errors='coerce'))
        scrapDate = pd.DataFrame({'date': datei}, index=range(len(tblByDate)))
        tblByDate = pd.concat([scrapDate.reset_index(drop=True), 
                            tblByDate.reset_index(drop=True)], axis=1)
        tblByDate['exchange'] = 'zse'
        zseList.append(tblByDate)
            
    # rbind all tables
    zseTrade = pd.concat(zseList, axis=0, sort=False)

    # add to database
    write_to_db(zseTrade, "odvjet12_stocks", "trade_zse")
