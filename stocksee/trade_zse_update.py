from utils import requests_retry_session
from utils import get_request
from utils import write_to_db
from utils import write_to_db_update
from utils import delete_rows
import pandas as pd
import janitor


if __name__ == '__main__':
    # get trade data for today
    today = pd.Timestamp.today().strftime('%Y-%m-%d')
    formData = {
        "selDatum": today,
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
        print("There are no trade data for ZSE.")
    else:
        # clean table
        tblByDate = janitor.clean_names(tblByDate)
        tblByDate = tblByDate.rename(columns={
            'ticker': 'symbol', 'change_%': 'change'})
        tblByDate['change'] = tblByDate['change'].str.extract('(.\d+,\d+)')
        tblByDate['change'] = tblByDate['change'].str.replace(',', '.')
        tblByDate.loc[:, 'close':'turnover'] = tblByDate.loc[:, 'close':'turnover']\
            .applymap(lambda x: pd.to_numeric(x, errors='coerce'))
        scrapDate = pd.DataFrame([pd.Timestamp.today()]*len(tblByDate),
                                columns=['date'])
        tblByDate = pd.concat([scrapDate.reset_index(drop=True), 
                            tblByDate.reset_index(drop=True)], axis=1)
        tblByDate['exchange'] = 'zse'
        
        # delete data for today
        delete_rows(f'DELETE FROM trade_zse WHERE date = {today};', 'trade_zse')
        
        # add today data to database
        write_to_db_update(tblByDate, 'odvjet12_stocks', 'trade_zse')
