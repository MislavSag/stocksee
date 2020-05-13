import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
import time
import janitor
import xmltodict
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


def unnesting(df, explode, axis):
    if axis==1:
        idx = df.index.repeat(df[explode[0]].str.len())
        df1 = pd.concat([
            pd.DataFrame({x: np.concatenate(df[x].values)}) for x in explode], axis=1)
        df1.index = idx

        return df1.join(df.drop(explode, 1), how='left')
    else :
        df1 = pd.concat([
                         pd.DataFrame(df[x].tolist(), index=df.index).add_prefix(x) for x in explode], axis=1)
        return df1.join(df.drop(explode, 1), how='left')
    
    
def extract_dict_value(df, column_name, dict_value='#text', prefix=''):
    if ~df[column_name].isna().all():
        new_column_name = prefix + column_name
        df[new_column_name] = [
            value[dict_value] if value is not np.nan and dict_value in value else np.nan 
            for index, value in df[column_name].iteritems()]
    else: 
        df[column_name] = np.nan
    return df



# connection
ib = IB()
ib.connect('127.0.0.1', 7496, clientId=1)

# import contracts
query = 'SELECT DISTINCT symbol, exchange, currency \
         FROM stocks_usa_details WHERE stockType = "COMMON" OR stockType = "PREFERRED"'
contracts = query_to_db(query, 'odvjet12_stocks')
print(np.where(contracts['symbol'] == 'NTEC'))
contracts = [Stock(row[0], row[1], row[2]) for index, row in contracts.iterrows()]

# get historical fundamental data
report_type = ['ReportsFinSummary', 'ReportSnapshot', 'ReportsFinStatements']


### FOR TEST
con = contracts[0]  # 1854
con = Stock('FB', 'SMART', 'USD')

ib.reqHeadTimeStamp(vix, whatToShow='TRADES', useRTH=True)

bars = ib.reqHistoricalData(
    con,
    endDateTime='',
    durationStr='2 M',
    barSizeSetting='1 M',
    whatToShow='TRADES',
    useRTH=True,
    formatDate=1
)
df = util.df(bars)
# df.to_csv(SPY_DATA_PATH + '/spy_' + '2020-04-23' + '.csv', index=False, sep=';')

ticker = ib.reqMktData(con, '258')
print(ticker.fundamentalRatios)




### RAEPORTS SNAPSHOT

# initialize variables
fin_statement_info = []
coamap = []
annual_fin_statements = []
interim_fin_statements = []

for con in contracts:
    
    ### FOR TEST
    # con = contracts[0]  # 1854
    # con = Stock('FB', 'SMART', 'USD')
    print(con)
    
    snapshot = ib.reqFundamentalData(con, reportType='ReportSnapshot')
    
    if len(snapshot) == 0:
        continue
        
    # financial reports metadata
    data = xmltodict.parse(snapshot)['ReportSnapshot']
    data.keys()
    data['Ratios'].keys()
    data['Ratios']['Group']
    
    # skip interim if there are no fin statements
    if data['FinancialStatements'] == None:
       continue 







### FINANCIAL STATEMENTS

# initialize variables
fin_statement_info = []
coamap = []
annual_fin_statements = []
interim_fin_statements = []

for con in contracts:
    
    ### FOR TEST
    # con = contracts[1854]
    # con = Stock('FB', 'SMART', 'USD')
    print(con)
    
    fundData = ib.reqFundamentalData(con, reportType='ReportsFinStatements')
    
    if len(fundData) == 0:
        continue
        
    # financial reports metadata
    data = xmltodict.parse(fundData)['ReportFinancialStatements']
    
    # skip interim if there are no fin statements
    if data['FinancialStatements'] == None:
       continue 

    # meta data
    company = pd.DataFrame.from_dict(data['CoIDs']['CoID'])
    company = first_column_to_names(company)
    company['major'] = data['@Major']
    company['minor'] = data['@Minor']
    company['revision'] = data['@Revision']
    company.reset_index(drop=True, inplace=True)
    issueid = pd.DataFrame.from_dict(data['Issues']['Issue']['IssueID'])
    issueid = first_column_to_names(issueid)
    issueid['issueid'] = data['Issues']['Issue']['@ID']
    issueid['issuetype'] = data['Issues']['Issue']['@Type']
    issueid['issuedesc'] = data['Issues']['Issue']['@Desc']
    issueid['issueorder'] = data['Issues']['Issue']['@Order']
    issueid.reset_index(drop=True, inplace=True)
    general_info = pd.DataFrame.from_dict(data['CoGeneralInfo'])
    general_info = general_info.iloc[[1], :].reset_index(drop=True)
    statement_info = pd.DataFrame.from_dict(data['StatementInfo'])
    statement_info = statement_info.iloc[[1], :].reset_index(drop=True)
    notes = pd.DataFrame.from_dict(data['Notes']).reset_index(drop=True)
    fin_info = pd.concat([company, issueid, general_info,
                        statement_info, notes], axis=1)
    if 'ticker' not in fin_info.columns:
        fin_info['ticker'] = fin_info['displayric'].str.replace(r'(\..*)', '')
    fin_statement_info.append(fin_info)
    
    # financial statements annual
    coamap.append(pd.DataFrame.from_dict(data['FinancialStatements']['COAMap']['mapItem']))
    
    
    ############## CHANGE THIS; DON'T WANT OSAVE THIS TO GITHUB ##################
    # save yml as json
    json_path = "usa_annual_statements/" + fin_info['ticker'][0] + '_' + fin_info['instrumentpi'][0] + ".json" 
    with open(json_path,"w") as f:
        json.dump(data['FinancialStatements']['AnnualPeriods']['FiscalPeriod'],f)
    
    ############## CHANGE THIS; DON'T WANT OSAVE THIS TO GITHUB ##################
    
    # load json object
    with open(json_path) as f:
        d = json.load(f)
    # convert json to dataframe
    df = json_normalize(
        d ,record_path=['Statement', 'lineItem'],
        meta=['@EndDate', '@FiscalYear', '@Type', ['Statement', '@Type'], ['Statement', 'FPHeader']])
    df = extract_dict_value(df, 'Statement.FPHeader', 'PeriodLength', 'PeriodLength')
    df = extract_dict_value(df, 'Statement.FPHeader', 'StatementDate', 'StatementDate')
    df = extract_dict_value(df, 'Statement.FPHeader', 'periodType', 'periodType')
    df = extract_dict_value(df, 'periodTypeStatement.FPHeader', '#text', '')
    df = extract_dict_value(df, 'Statement.FPHeader', 'UpdateType', 'UpdateType')
    df = extract_dict_value(df, 'UpdateTypeStatement.FPHeader', '#text', '')
    df = extract_dict_value(df, 'Statement.FPHeader', 'AuditorName', 'AuditorName')
    df = extract_dict_value(df, 'AuditorNameStatement.FPHeader', '#text', '')
    df = extract_dict_value(df, 'Statement.FPHeader', 'AuditorOpinion', 'AuditorOpinion')
    df = extract_dict_value(df, 'AuditorOpinionStatement.FPHeader', '#text', '')
    df = extract_dict_value(df, 'Statement.FPHeader', 'Source', 'Source')
    df = extract_dict_value(df, 'SourceStatement.FPHeader', '@Date', '')
    df.drop(columns=['Statement.FPHeader'], inplace=True)
    df.drop(columns=['Statement.@Type'], inplace=True)
    df['#text'] = df['#text'].astype(float)
    df = df.loc[:, ~df.isna().all()]
    id_cols = [col for col in df.columns if col not in ['@coaCode', '#text']]
    df = df.pivot_table(
        values='#text',
        index=id_cols,
        columns='@coaCode'
        )
    df.reset_index(drop=False, inplace=True)
    df_fin_info = pd.concat([fin_info[['repno', 'instrumentpi', 'ticker']], 
                             df.set_index(pd.Series([0]).repeat(df.shape[0]))],
                            axis=1)
    annual_fin_statements.append(df_fin_info)
    
    # skip interim if it doesnt exist
    if data['FinancialStatements']['InterimPeriods'] == None:
       continue 
    
    ############## CHANGE THIS; DON'T WANT OSAVE THIS TO GITHUB ##################
    
    # save yml as json
    json_path = "usa_interim_statements/" + fin_info['ticker'][0] + '_' + fin_info['instrumentpi'][0] + ".json"
    with open(json_path,"w") as f:
        json.dump(data['FinancialStatements']['InterimPeriods']['FiscalPeriod'],f)
        
    ############## CHANGE THIS; DON'T WANT OSAVE THIS TO GITHUB ##################
    # load json object
    with open(json_path) as f:
        d = json.load(f)
    # convert json to dataframe
    df = json_normalize(d ,record_path=['Statement', 'lineItem'],
                        meta=['@EndDate', '@FiscalYear', '@Type', ['Statement', '@Type'], ['Statement', 'FPHeader']])
    df = extract_dict_value(df, 'Statement.FPHeader', 'PeriodLength', 'PeriodLength')
    df = extract_dict_value(df, 'Statement.FPHeader', 'StatementDate', 'StatementDate')
    df = extract_dict_value(df, 'Statement.FPHeader', 'periodType', 'periodType')
    df = extract_dict_value(df, 'periodTypeStatement.FPHeader', '#text', '')
    df = extract_dict_value(df, 'Statement.FPHeader', 'UpdateType', 'UpdateType')
    df = extract_dict_value(df, 'UpdateTypeStatement.FPHeader', '#text', '')
    df = extract_dict_value(df, 'Statement.FPHeader', 'AuditorName', 'AuditorName')
    df = extract_dict_value(df, 'AuditorNameStatement.FPHeader', '#text', '')
    df = extract_dict_value(df, 'Statement.FPHeader', 'AuditorOpinion', 'AuditorOpinion')
    df = extract_dict_value(df, 'AuditorOpinionStatement.FPHeader', '#text', '')
    df = extract_dict_value(df, 'Statement.FPHeader', 'Source', 'Source')
    df = extract_dict_value(df, 'SourceStatement.FPHeader', '@Date', '')
    df.drop(columns=['Statement.FPHeader'], inplace=True)
    df.drop(columns=['Statement.@Type'], inplace=True)
    df['#text'] = df['#text'].astype(float)
    df = df.loc[:, ~df.isna().all()]
    id_cols = [col for col in df.columns if col not in ['@coaCode', '#text']]
    df = df.pivot_table(
        values='#text',
        index=id_cols,
        columns='@coaCode'
        )        
    df.reset_index(drop=False, inplace=True)
    df_fin_info_interim = pd.concat([fin_info[['repno', 'instrumentpi', 'ticker']], 
                                     df.set_index(pd.Series([0]).repeat(df.shape[0]))],
                                    axis=1)
    interim_fin_statements.append(df_fin_info_interim)
    
    # wait! : https://interactivebrokers.github.io/tws-api/fundamentals.html
    time.sleep(10)



# merge everything in one table
fin_statement_info_all = pd.concat(fin_statement_info, axis=0)
coamap_all = pd.concat(coamap, axis=0)
coamap_all = coamap_all.drop_duplicates(ignore_index=True)
coamap_all.set_axis(coamap_all.columns.str.replace(r'@|#', ''), axis='columns', inplace=True)
# annual financial statements
annual_fin_statements_all = pd.concat(annual_fin_statements, axis=0)
annual_fin_statements_all = janitor.clean_names(annual_fin_statements_all)
annual_fin_statements_all.set_axis(
    annual_fin_statements_all.columns.str.replace(r'@|#', ''), axis='columns', inplace=True)
# interim financial statements
interim_fin_statements_all = pd.concat(interim_fin_statements, axis=0)
interim_fin_statements_all = janitor.clean_names(interim_fin_statements_all)
interim_fin_statements_all.set_axis(
    interim_fin_statements_all.columns.str.replace(r'@|#', ''), axis='columns', inplace=True)

# add to database
write_to_db(fin_statement_info_all, "odvjet12_stocks", "fundaments_usa_info")
write_to_db(coamap_all, "odvjet12_stocks", "fundaments_usa_coamap")
write_to_db(annual_fin_statements_all, "odvjet12_stocks", "fundaments_usa_annual")
write_to_db(interim_fin_statements_all, "odvjet12_stocks", "fundaments_usa_interim")



ib.disconnect()
