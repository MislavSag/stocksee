from utils import requests_retry_session
from utils import get_request
from utils import write_to_db
from utils import write_to_db_update
from utils import delete_rows
from utils import query_to_db
import re
from datetime import date
import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import janitor
import xlrd


### GLOBALS
fonds = ["BRIN", "FMPS", "SLPF", "QUNE", "NFDA"]


def get_fin_stat_links():
    """
    Get urls of new financial statements news from
    Source: https://zse.hr/default.aspx?id=36774&ticker1=&Page=1
    """
    url =  'https://zse.hr/default.aspx?id=36774&ticker1=&Page=1'
    res = get_request(url)
    bs = BeautifulSoup(res.text, features="lxml")
    link = [a['href'] for a in bs.find_all("a", href=True)]
    link = ["http://www.zse.hr/" + l for l in link if "UserDocsImages/financ" in l]
    dtime = [date.get_text().strip() for date in bs.select('.vijestRowDatumDat')]
    dtime = pd.to_datetime(dtime)
    
    return dtime, link


def get_metadata_from_links(links):
    """
    Get metadata from the links from ZSE site.
    :param links: (list) list of strings (links) 
    """
    gfiMetadata = [re.sub(r".*financ/", "", l) for l in links]
    gfiMetadata = pd.DataFrame(gfiMetadata, columns=["link"])
    gfiMetadata = gfiMetadata["link"].str.split(pat="-", n=6, expand=True)
    gfiMetadata = pd.concat([pd.DataFrame(link), gfiMetadata], axis=1)
    gfiMetadata.set_index(dtime, inplace=True)
    gfiMetadata.columns = ["link", "symbol", "year", "quarter", "rev",
                           "cons", "country"]
    gfiMetadata = (gfiMetadata[gfiMetadata['link'].
                            str.contains('xlsx|xls', case=False)])
    
    return gfiMetadata


def clean_gfi_table(gfi_meta_table):
    # continue if there are new financial statements - clean gfi metadata table
    gfiMeta = gfi_meta_table.dropna(subset=["symbol"])
    gfiMeta = gfiMeta[[metadta not in urls for metadta in gfi_meta_table['link']]] # keep only new
    gfiMeta['datetime'] = gfiMeta.index
    gfiMeta['year'] = gfiMeta['year'].str.extract(r'(\d+)').astype(int)
    gfiMeta['quarter'] = gfiMeta['quarter'].str.replace(r"\.xls", "", case=False)
    gfiMeta.replace({
        "quarter" : {"12" : "4Q", "09" : "3Q", "06" : "2Q", "03" : "1Q",
                     "31" : "1Y", "1H\(2\)" : "1H", "3O" : "3Q"}
        })
    gfiMeta['quarter'] = np.where(gfiMeta['quarter'] == "", "1Y", gfiMeta['quarter'])
    gfiMeta['rev'] = np.where(
        gfiMeta['rev'].str.contains(r"nerev|notrev", flags=re.IGNORECASE),
        str(0), "")
    gfiMeta['rev'] = np.where(
        gfiMeta['link'].str.
        contains(r"rev", flags=re.IGNORECASE).tolist() and
        (gfiMeta['rev'] == "").tolist(), str(1), gfiMeta['rev']).astype(int)
    gfiMeta['cons'] = np.where(
        gfiMeta['cons'].str.contains(r'-N-|nekon|N', flags=re.IGNORECASE),
        str(0), "")
    gfiMeta['cons'] = np.where(
        gfiMeta['link'].str.contains(r"-K-|kon|K", flags=re.IGNORECASE).tolist() and 
        (gfiMeta['rev'] == "").tolist(), str(1), gfiMeta['rev']).astype(int)
    gfiMeta['ext'] = gfiMeta['link'].str.extract(r'((?<=\.)[^.]*$)', expand=False)
    gfiMeta['country'] = "HR"
    cols = ['datetime'] + [col for col in gfiMeta if col != 'datetime']
    gfiMeta = gfiMeta[cols]
    gfiMeta['datetime'] = gfiMeta['datetime'].astype(str)
    gfiMeta['filename'] = gfiMeta['link'].str.replace(r".*/", "")
    
    return gfiMeta


def fin_stat_tag_converter(type_fs):
    """
    Convert type of financial statements to pattern that appears in excel
    sheets.
    """
    if type_fs is "bilanca":
        tip_gfi_pattern = r"bil|balance|bs"
    elif type_fs is "rdg":
        tip_gfi_pattern = r"rdg|rdg |rdig|pl ht|p&l|PL|pl"
    elif type_fs is "nt":
        tip_gfi_pattern = r"nt_i|novčani|novcani|○cf|int|nt-i|^nt$|cash"
    elif type_fs is "nd":
        tip_gfi_pattern = r"nt_d|dnt|dir|nt-d|\(d\)"
        
    return tip_gfi_pattern


def import_excel(file_name, symbol, gfi_type):
    """
    Import excel files web scraped from ZSE.
    Import only data frmo one sheet.
    """
    # get sheet pattern
    sheet_pattern = fin_stat_tag_converter(gfi_type)

    # import excel from directory
    xl = pd.ExcelFile('zse_fs/' + file_name)

    # if excel file contain only 2 sheets: Podaci and RefStr, cont.
    if all([sheet in xl.sheet_names for sheet in ["Podaci", "RefStr"]]):
        return None
    sheets = list(map(str.lower, xl.sheet_names))
    sheets = [re.sub("bilješke", "dsfafafdsafdsa", sheet) for sheet in sheets]

     # if all sheet names are "liste" append and cont
    if all([sheets == "liste" for sheet in sheets]):
        return "liste"
    reportIndex = [i for i, sheet in enumerate(sheets)
                   if re.match(sheet_pattern, sheet)]

    # continue if no matched sheet
    if len(reportIndex) == 0:
        return None

    # if identify two sheets, keep only first
    if len(reportIndex) >= 2:
        reportIndex = [reportIndex[0]]
    if symbol in fonds:
        return "fond"

    # import excel sheet
    df = pd.read_excel('zse_fs/' + file_name, sheet_name=reportIndex[0])
    
    # return None it df is empty (no data on FS)
    if len(df) == 0:
     return None
    
    return df


def remove_rbr_columns(df):
    # remove columns that contain rbr
    rbr = df.apply(lambda x: 
        x.astype(str).str.contains(r'(Rbr\.|Red\. br\.)')).any()
    df = df.loc[:, ~rbr]

    # if have only 2 columns and have only 0, return None
    if df.shape[1] == 2 & ((all((df == 0).all())) | 
                            all(df.isna().all())):
        return None
        
    return df


def extract_aop(df):
    # first column from which to extract data
    # in which column is AOP (not 1.)
    where_aop = (df.iloc[:, 1:].
                apply(lambda x: x.astype(str).str.contains('AOP|ADP')).any())
    where_aop = where_aop.iloc[:11] # nekada se pojavljuje u nekom novoj zadnjoj koloni, na 18 mjestu
    where_aop = np.where(where_aop)[0] + 1  # uvecati za jedan jer sam maknuo prvi redak u prethodnom koraku
    where_aop = where_aop[-1] # moguce je da se AOP spominje vise puta, gledati zadnji AOP
    if not isinstance(where_aop, np.int64):
        where_aop = (df.iloc[:, 1:].
                    apply(lambda x: x.astype(str).str.contains('Prethodno')).any())
    if not isinstance(where_aop, np.int64):
        where_aop = np.where(df.dtypes == 'float64')[0][0]
    if not isinstance(where_aop, np.int64):
        return None
    
    # define first row from which to extract data
    first_row = (df.iloc[:, where_aop].
                str.match("AOP|ADP|Prethodno|AKTIVA|Naziv pozicije"))
    first_row = np.where(first_row == True)[0] + 1
    if len(first_row) == 0:
        first_row = first_row = (df.iloc[:, where_aop].
                str.match("Naziv.*pozicije"))
        first_row = np.where(first_row == True)[0] + 1
    
    # extract main table
    df = df.iloc[first_row[0]:, where_aop:]
    df = df.loc[df.iloc[:, 0].astype(str).str.contains(r'(\d+)')]
    df = df.loc[:, ~(df.isna().sum() > 100)] # izbaciti kolone koje imaju vecinu NA
    
    return df


def remove_unnecesary_columns(df):
    # remove unnecesary columns
    if isinstance(df, pd.Series): # ovo se pojavi kada su sve vrijednosti u izvještaju NA (R)
        return None
    #   if (isTRUE(all.equal(df[1, ], c(2:6, "6.mj", "09-06mj"), check.attributes = FALSE))) {
    #     df <- df[, 1:(length(df)-2)]} # maknuti nepotrebne kolone, za jedan xls samo
    if df.iloc[0, 0] == 2:
        test1 = df.shape[1] > 3
        test2 = df.iloc[0, :].isna().any()
        test3 = df.loc[:, df.iloc[0, :].notna()].shape[1] > 1
        if test1 and test2 and test3:
            df = df.loc[:, df.iloc[0, :].notna()]
        test4 = df.iloc[0, :].duplicated().any()
        test5 = df.drop_duplicates().shape[1] > 2
        if test1 and test4 and test5:
            df = df.drop_duplicates()  # izbaciti ako se broj u koloni ponavlja
        df = df.iloc[1:, :]
        
    return df


def remove_unnecesary_rows(df):
    # remove unnecesary rows
    if df.shape[1] >= 3:
        df = df.loc[~((df.iloc[:, 0] == 2) & ((df.iloc[:, 2] == 3) & (df.iloc[:, 2] == 4))), ]
        df = df.loc[~((df.iloc[:, 0] == 2) & ((df.iloc[:, 2] == 3) & (df.iloc[:, 2] == 5))), ]
        df = df.loc[~((df.iloc[:, 0] == 1) & ((df.iloc[:, 2] == 2) & (df.iloc[:, 2] == 3))), ]
        df = df.loc[~((df.iloc[:, 0] == 3) & ((df.iloc[:, 2] == 4) & (df.iloc[:, 2] == 5))), ]
    else:
        df = df.loc[~((df.iloc[0, :] == 2) & (df.iloc[1, :] == 3))]
        
    return df


def swap_rows(df, gfi_type):
    # swap rows
    test1 = df.iloc[:, 0].duplicated().any()
    test2 = any((df.loc[df.iloc[:, 0].duplicated()] == 214).any())
    test3 = gfi_type == 'rdg'
    if test1 and test2 and test3:
        df.drop_duplicates(inplace=True)
    # cont if necessary
    
    return df


def check_aop_missing(df):
    # check if some number missing
    numMiss = np.setdiff1d(np.arange(df.iloc[0, 0], df.iloc[0, 0]+df.shape[0]-1),
                        df.iloc[:, 0].values)
    if len(numMiss) > 0:
        df.ioc[np.arrange(df.shape[0]+1, df.shape[0]+len(numMiss)), 0] = numMiss
        
    return df


def change_type_and_name(df, metadata, gfi_type, i):
    # change column types
    df.iloc[:, 1:] = df.iloc[:, 1:].astype('float').fillna(0)
    
    # set column names
    df = df.T
    df.columns = df.iloc[0, :]
    df = df.iloc[1:, :]
    df.columns = df.columns.astype(str).str.pad(3, 'left', fillchar='0')
    df = df.add_prefix('b')
    # df.columns = [('b' + str(i).zfill(3)) for i in list(range(1, (df.shape[1]+1)))]
    metaToMerge = metadata.iloc[[i], 2:8].reset_index(drop=True)
    df.index = np.repeat(0, df.shape[0])
    df = pd.concat([df, metaToMerge], axis=1)
    df['tip_izvjestaja'] = gfi_type
    
    return df


def set_lag_year(df):
    # we have 2 years; set previous year to year-1
    if df.shape[0] > 1:
        previousYear = df.iloc[range(int((df.shape[0]/2)),
                                     df.shape[0])].loc[:, 'year'] - 1
        df['year'].iloc[range(int(df.shape[0]/2))] = previousYear
    df['report_year'] = df.year.max()
    
    return df
        

def add_lavels_insurance_inc(df, gfi_type):
    # add special labels for insurance companies
    if df.shape[0] == 4 and gfi_type == 'rdg':
        df['kumulativno'] = 0
        df['kumulativno'].iloc[np.arange(0, df.shape[0], 2)] = 1
    elif df.shape[0] == 6 and gfi_type == 'rdg':
        df['vrsta_osiguranja'] = np.nan
        df['vrsta_osiguranja'].iloc[np.arange(0, df.shape[0], 3)] = "zivot"
        df['vrsta_osiguranja'].iloc[np.arange(1, df.shape[0], 3)] = "nezivot"
        df['vrsta_osiguranja'].iloc[np.arange(2, df.shape[0], 3)] = "ukupno"
    elif df.shape[0] == 6 and gfi_type == 'bilanca':
        df['vrsta_osiguranja'] = np.nan
        df['vrsta_osiguranja'].iloc[np.arange(0, df.shape[0], 3)] = "zivot"
        df['vrsta_osiguranja'].iloc[np.arange(1, df.shape[0], 3)] = "nezivot"
        df['vrsta_osiguranja'].iloc[np.arange(2, df.shape[0], 3)] = "ukupno"
        
    return df


################ DEBUG #################    
metadata = gfi_metadata.copy()
gfi_type = 'nd'
for index, row in metadata[['filename', 'symbol']].iterrows():
    """
    Import excel files web scraped from ZSE.
    Import only data frmo one sheet.
    """
    print(index)
    
    # get sheet pattern
    sheet_pattern = fin_stat_tag_converter(gfi_type)

    # import excel from directory
    print()
    xl = pd.ExcelFile('zse_fs/' + 'JDOS-fin2020-1Q-NotREV-K-HR.xlsx')
    xl = pd.ExcelFile('zse_fs/' + row['filename'])

    # if excel file contain only 2 sheets: Podaci and RefStr, cont.
    if all([sheet in xl.sheet_names for sheet in ["Podaci", "RefStr"]]):
        continue
    sheets = list(map(str.lower, xl.sheet_names))
    sheets = [re.sub("bilješke", "dsfafafdsafdsa", sheet) for sheet in sheets]

     # if all sheet names are "liste" append and cont
    if all([sheets == "liste" for sheet in sheets]):
        continue
    reportIndex = [i for i, sheet in enumerate(sheets)
                   if re.match(sheet_pattern, sheet)]

    # continue if no matched sheet
    if len(reportIndex) == 0:
        continue

    # if identify two sheets, keep only first
    if len(reportIndex) >= 2:
        reportIndex = [reportIndex[0]]
    if row['symbol'] in fonds:
        continue

    # import excel sheet
    print(reportIndex)
    df = pd.read_excel('zse_fs/' + row['filename'], sheet_name=reportIndex[0])
    
    # return None it df is empty (no data on FS)
    if len(df) == 0:
     return None
    
    return df


################ DEBUG #################    


def clean_scraped_excel_tables(metadata, gfi_type):
    xls = [import_excel(row['filename'], row['symbol'], gfi_type) 
        for index, row in metadata[['filename', 'symbol']].iterrows()]
    clean_df = [df for df in xls if df is not 'fond']
    clean_df = [remove_rbr_columns(df) for df in clean_df if df is not None]
    clean_df = [extract_aop(df) for df in clean_df if df is not None]
    clean_df = [remove_unnecesary_columns(df) for df in clean_df if df is not None]
    clean_df = [remove_unnecesary_rows(df) for df in clean_df if df is not None]
    clean_df = [swap_rows(df, gfi_type) for df in clean_df if df is not None]
    clean_df = [check_aop_missing(df) for df in clean_df if df is not None]
    clean_df = [change_type_and_name(df, metadata, gfi_type, i)
                for i, df in enumerate(clean_df) if df is not None]
    clean_df = [set_lag_year(df) for df in clean_df if df is not None]
    clean_df = [add_lavels_insurance_inc(df, gfi_type) for df in clean_df if df is not None]
    
    return clean_df


def clean_balance_sheet(metadata):
    bilanca = clean_scraped_excel_tables(metadata, 'bilanca')
    bilanca = [b for b in bilanca if b is not 'fond']
    for b in bilanca:
        if b.shape[1] == 116:
            b[['b109', 'b110']] = 0
    # add reports type
    for b in bilanca:
        aopLength = len(b.columns.str.extract(r'(b\d+)').dropna())
        if aopLength == 124:
            b['format_subjekta'] = "firma_2018"
        elif aopLength == 110:
            b['format_subjekta'] = "firma_2010"
        elif aopLength == 34:
            b['format_subjekta'] = "firma_2009"
        elif aopLength == 106:
            b['format_subjekta'] = "firma_2009_det"
        elif aopLength == 68:
            b['format_subjekta'] = "banka_2018"
        elif aopLength == 47:
            b['format_subjekta'] = "banka_2009_det"
        elif aopLength == 40:
            b['format_subjekta'] = "banka_2009"
        elif aopLength == 117:
            b['format_subjekta'] = "osiguranje_2018"
        elif aopLength == 123:
            b['format_subjekta'] = "osiguranje_2010"
        elif aopLength == 49:
            b['format_subjekta'] = "osiguranje_2009"
        elif aopLength == 54:
            b['format_subjekta'] = "zb"
            
    return bilanca


def clean_pl(metadata):
    rdg = clean_scraped_excel_tables(metadata, 'rdg')
    rdg = [r for r in rdg if r is not 'fond']
    for r in rdg:
        if r.shape[1] == 67:
            r[['b169', 'b170']] = 0
        if r.columns.str.match('kumulativno').any():
            r['kumulativno'] = 0

    # add reports type
    for r in rdg:
        aopLength = len(r.columns.str.extract(r'(b\d+)').dropna())
        if aopLength == 93 and r.columns[0] == "b125":
            r['format_subjekta'] = "firma_2018"
        elif aopLength == 60:
            r['format_subjekta'] = "firma_2010"
        elif aopLength == 38:
            r['format_subjekta'] = "firma_2009"
        elif aopLength == 46:
            r['format_subjekta'] = "firma_2009_det"
        elif aopLength == 61:
            r['format_subjekta'] = "banka_2018"
        elif aopLength == 34:
            r['format_subjekta'] = "banka_2009"
        elif aopLength == 28:
            r['format_subjekta'] = "banka_2009_det"
        elif aopLength == 80:
            r['format_subjekta'] = "osiguranje_2018"
        elif aopLength == 93 and r.columns[0] == "b124":
            r['format_subjekta'] = "osiguranje_2010"
        elif aopLength == 33:
            r['format_subjekta'] = "osiguranje_2009"
        elif aopLength == 57:
            r['format_subjekta'] = "zb_2018"
            
    return rdg


def clean_nt(metadata):
    ntd = clean_scraped_excel_tables(metadata, 'nt')
    ntd = [n for n in ntd if n is not 'fond']
    ntd = [n for n in ntd if n.shape[1] is not 37]

    # add reports type
    for i, n in enumerate(ntd):
        aopLength = len(n.columns.str.extract(r'(b\d+)').dropna())
        if aopLength == 50:
            n['format_subjekta'] = "firma_2018"
        elif aopLength == 44:
            n['format_subjekta'] = "firma_2010"
        elif aopLength == 42:
            n['format_subjekta'] = "firma_2018_HHLD"
        elif aopLength == 42:
            n['format_subjekta'] = "firma_2010_NEPOZNATO"
        elif aopLength == 51:
            n['format_subjekta'] = "banka_2018"
        elif aopLength == 44:
            n['format_subjekta'] = "banka_2010"
        elif aopLength == 57 and (n['report_year'] == 2019).all():
            n['format_subjekta'] = "osiguranje_2018"
        elif aopLength == 57:
            n['format_subjekta'] = "osiguranje_2010"
        elif aopLength == 40:
            n['format_subjekta'] = "zb_2010"
            
    return ntd


def clean_ntd(metadata):
    ntd = clean_scraped_excel_tables(metadata, 'nd')
    ntd = [n for n in ntd if n is not 'fond']
    ntd = [n for n in ntd if n.shape[1] is not 39]

    # add reports type
    ############ TEST
    n = ntd[0]
    ############ TEST
    for n in ntd:
        aopLength = len(n.columns.str.extract(r'(b\d+)').dropna())
        if aopLength == 50:
            n['format_subjekta'] = "firma_2018"
        elif aopLength == 44:
            n['format_subjekta'] = "firma_2010"
        elif aopLength == 42:
            n['format_subjekta'] = "firma_2018_HHLD"
        elif aopLength == 42:
            n['format_subjekta'] = "firma_2010_NEPOZNATO"
        elif aopLength == 51:
            n['format_subjekta'] = "banka_2018"
        elif aopLength == 44:
            n['format_subjekta'] = "banka_2010"
        elif aopLength == 57 and (n['report_year'] == 2019).all():
            n['format_subjekta'] = "osiguranje_2018"
        elif aopLength == 57:
            n['format_subjekta'] = "osiguranje_2010"
        elif aopLength == 40:
            n['format_subjekta'] = "zb_2010"
            
    return ntd


def col_create(main, prefix='abc'):
    return [prefix + str(x).zfill(3) for x in main]


def convert_2018_to_2010_bilanca(bilanca):
    # Bilanca
    bilancaSkupa = [b.copy() for b in bilanca 
                    if (b['format_subjekta'] == 'firma_2018').any()]
    for b in bilancaSkupa:
        b[col_create(list(range(1, 21)))] = b[col_create(list(range(1, 21)), 'b')]
        b['abc021'] = b['b021'] + b['b022']
        b['abc022'] = b['b023']
        b['abc023'] = b['b024']
        b['abc024'] = b['b025'] + b['b026']
        b[col_create(list(range(25, 30)))] = b[col_create(list(range(27, 32)), 'b')]
        b['abc030'] = b['b032'] + b['b033']
        b['abc031'] = b['b034']
        b['abc032'] = b['b035']
        b[col_create(list(range(33, 45)))] = b[col_create(list(range(36, 48)), 'b')]
        b['abc045'] = b['b049']
        b['abc046'] = b['b048']
        b[col_create(list(range(47, 51)))] = b[col_create(list(range(50, 54)), 'b')]
        b['abc051'] = b['b054'] + b['b055']
        b['abc052'] = b['b056']
        b['abc053'] = b['b057'] + b['b058']
        b[col_create(list(range(54, 70)))] = b[col_create(list(range(59, 75)), 'b')]
        b['abc070'] = b['b075'] + b['b077']
        b['abc071'] = b['b076']
        b[col_create(list(range(72, 82)))] = b[col_create(list(range(81, 91)), 'b')]
        b['abc082'] = b['b091'] + b['b092'] + b['b093'] + b['b094']
        b['abc083'] = b['b095']
        b['abc084'] = b['b096'] + b['b097']
        b[col_create(list(range(85, 90)))] = b[col_create(list(range(100, 105)), 'b')]
        b['abc090'] = b['b098'] + b['b099']
        b[col_create(list(range(91, 94)))] = b[col_create(list(range(105, 108)), 'b')]
        b['abc094'] = b['b108'] + b['b109']
        b[col_create(list(range(95, 100)))] = b[col_create(list(range(112, 117)), 'b')]
        b['abc100'] = b['b110'] + b['b111']
        b[col_create(list(range(101, 109)))] = b[col_create(list(range(117, 125)), 'b')]
        b['abc109'] = 0
        b['abc110'] = 0
    bilancaSkupa = [tbl.loc[:, ~tbl.columns.str.startswith('b')] for tbl in bilancaSkupa]
    bilancaSkupa = pd.concat(bilancaSkupa, axis=0)
    bilancaSkupa.columns = bilancaSkupa.columns.str.replace(r"abc", "b")
    bilancaSkupa[['format_subjekta']] = "firma_2010"

    bilanca = pd.concat(bilanca, axis=0)
    bilanca = pd.concat([bilanca, bilancaSkupa], axis=0)
    
    return bilanca



def convert_2018_to_2010_rdg(rdg):
    # PL
    rdgSkupa = [b.copy() for b in pl if (b['format_subjekta'] == 'firma_2018').any()]
    for b in rdgSkupa:
        b['abc111'] = b['b125']
        b['abc112'] = b['b126'] + b['b127']
        b['abc113'] = b['b128'] + b['b129'] + b['b130']
        b[col_create(list(range(114, 130)))] = b[col_create(list(range(131, 147)), 'b')]
        b[col_create(list(range(130, 132)))] = b[col_create(list(range(153, 155)), 'b')]
        b['abc132'] = b['b155'] +b['b157'] + b['b158'] + b['b159']
        b['abc133'] = b['b168'] + b['b169']  + b['b171']
        b['abc134'] = b['b156']
        b[col_create(list(range(135, 138)))] = b[col_create(list(range(163, 166)), 'b')]
        b['abc138'] = b['b166'] +b['b167']
        b['abc139'] = b['b160'] + b['b161']  + b['b162']
        b['abc140'] = b['b170']
        b['abc141'] = b['b172']
        b['abc142'] = b['b173'] + b['b175']
        b['abc143'] = b['b174'] + b['b176']
        b['abc144'] = 0
        b['abc145'] = 0
        b[col_create(list(range(146, 148)))] = b[col_create(list(range(177, 179)), 'b')]
        b['abc148'] = b['b178'] + b['b186']
        b['abc149'] = b['b180'] + b['b187']
        b['abc150'] = b['b181'] + b['b188']
        b['abc151'] = b['b182'] + b['b189']
        b['abc152'] = b['b199']
        b['abc153'] = b['b184'] + b['b190']
        b['abc154'] = b['b185'] + b['b191']
        b[col_create(list(range(155, 157)))] = b[col_create(list(range(200, 202)), 'b')]
        b['abc157'] = b['b159']
        b[col_create(list(range(158, 165)))] = b[col_create(list(range(203, 210)), 'b')]
        b['abc165'] = b['b210'] + b['b211']
        b[col_create(list(range(166, 169)))] = b[col_create(list(range(212, 215)), 'b')]
        b[col_create(list(range(169, 171)))] = b[col_create(list(range(216, 218)), 'b')]
        
    rdgSkupa = [tbl.loc[:, ~tbl.columns.str.startswith('b')] for tbl in rdgSkupa]
    rdgSkupa = pd.concat(rdgSkupa, axis=0)
    rdgSkupa.columns = rdgSkupa.columns.str.replace(r"abc", "b")
    rdgSkupa[['format_subjekta']] = "firma_2010"
    rdg = pd.concat(rdg, axis=0)
    rdg = pd.concat([rdg, rdgSkupa], axis=0)
    
    return rdg


def convert_2018_to_2010_ntd(ntd):
    # NTD
    ntdSkupa = [b.copy() for b in ntd if (b['format_subjekta'] == 'firma_2018').any()]
    for b in ntdSkupa:
        b['abc001'] = b['b001']  # 1. Profit before tax
        b['abc002'] = b['b003']  # 2. Depreciation and amortisation
        b['abc003'] = np.where(b['b013'] >= 0, np.abs(b['b013']), 0)  # 3. Increase in short-term liabilities
        b['abc004'] = np.where(b['b014'] >= 0, np.abs(b['b014']), 0)  # 4. Decrease in short-term receivables
        b['abc005'] = np.where(b['b015'] >= 0, np.abs(b['b015']), 0)  # 5. Decrease in inventories
        b['abc006'] = np.where(b['b016'] >= 0, np.abs(b['b016']), 0) + \
            np.where((b['b011'] - b['b001'] - b['b003']) >= 0,
                    np.abs(b['b011'] - b['b001'] - b['b003']), 0) + \
            np.where(b['b018'] >= 0, np.abs(b['b018']), 0) + \
            np.where(b['b019'] >= 0, np.abs(b['b019']), 0)   # 6. Other cash flow increases
        b['abc007'] = b['b001'] + b['b003'] + b['abc003'] + b['abc004'] +\
            b['abc005'] + b['abc006']
        b['abc008'] = np.where(b['b013'] <= 0, np.abs(b['b013']), 0)  # 1. Decrease in short-term liabilities
        b['abc009'] = np.where(b['b014'] <= 0, np.abs(b['b014']), 0)  # 2. Increase in short-term receivables
        b['abc010'] = np.where(b['b015'] <= 0, np.abs(b['b015']), 0)  # 3. Increase in inventories
        b['abc011'] = np.where(b['b016'] <= 0, np.abs(b['b016']), 0) + \
            np.where((b['b011'] - b['b001'] - b['b003']) <= 0,
                    np.abs(b['b011'] - b['b001'] - b['b003']), 0) + \
            np.where(b['b018'] <= 0, np.abs(b['b018']), 0) + \
            np.where(b['b019'] <= 0, np.abs(b['b019']), 0)   # 6. Other cash flow increases
        b['abc012'] = b['b008'] + b['b009'] + b['abc010'] + b['abc011']

        b['abc015'] = np.abs(b['b021'])  # 1. Novčani primici od prodaje dugotrajne materijalne i nematerijalne imovine
        b['abc016'] = np.abs(b['b022'])  # 2. Cash inflows from sale of equity and debt instruments
        b['abc017'] = np.abs(b['b023'])  # 3. Interest receipts
        b['abc018'] = np.abs(b['b024'])  # 4. Dividends receipts
        b['abc019'] = b['b025'] + b['b026']  # 5. Other cash inflows from investment activities
        b['abc020'] = np.abs(b['b027'])  # III. Total cash inflows from investment activities (015 till 019)
        b['abc021'] = np.abs(b['b028'])  # 1. Cash outflows for purchase of long-term tangible and intangible assets
        b['abc022'] = np.abs(b['b029'])  # 2. Cash outflows for purchase of equity and debt financial instruments
        b['abc023'] = np.abs(b['b030'] + b['b031'] + b['b032'])  # 3. Other cash outflows from investment activities
        b['abc024'] = np.abs(b['b033'])  # IV. Total cash outflows from investment activities (021 till 023)
        
        b['abc027'] = b['b035'] + b['b036']  # 1. Cash receipts from issuance from equity and debt financial instruments
        b['abc028'] = np.abs(b['b037'])   # 2. Cash inflows from loans, debentures, credits and other borrowings
        b['abc029'] = np.abs(b['b038'])  # 3. Other cash inflows from financial activities
        b['abc030'] = np.abs(b['b039'])  # V. Total cash inflows from financial activities (027 till 029)
        b['abc031'] = np.abs(b['b040'])  # 1. Cash outflows for repayment of loans and bonds
        b['abc032'] = np.abs(b['b041'])  # 2. Dividends paid
        b['abc033'] = np.abs(b['b042'])  # 3. Cash outflows for finance lease
        b['abc034'] = np.abs(b['b043'])  # 4. Cash outflows for purchase of own stocks
        b['abc035'] = np.abs(b['b044'])  # 5. Other cash outflows from financial activities
        b['abc036'] = np.abs(b['b045'])  # VI. Total cash outflows from financial activities (031 till 035)
        
        b['abc041'] = np.abs(b['b049'])  # Cash and cash equivalents at the beginning of the period
        b['abc042'] = np.where(b['b048'] >= 0, b['b048'], 0)  # Increase of cash and cash equivalents
        b['abc043'] = np.where(b['b048'] <= 0, np.abs(b['b048']), 0)   # Decrease of cash and cash equivalents
        b['abc044'] = np.abs(b['b050'])  # Cash and cash equivalents at the end of the period
        
        b['abc013'] = np.where((b['abc007'] - b['abc012']) >= 0,
                            np.abs(b['abc007'] - b['abc012']), 0)  # A1) NET INCREASE OF CASH FLOW FROM OPERATING ACTIVITIES
        b['abc014'] = np.where((b['abc007'] - b['abc012']) <= 0,
                            np.abs(b['abc007'] - b['abc012']), 0)  # A2) NET DECREASE OF CASH FLOW FROM OPERATING ACTIVITIES (012-007)
        b['abc025'] = np.where((b['abc020'] - b['abc024']) >= 0,
                            np.abs(b['abc010'] - b['abc024']), 0)  # B1) NET INCREASE OF CASH FLOW FROM INVESTMENT ACTIVITIES (020-024)
        b['abc026'] = np.where((b['abc020'] - b['abc024']) <= 0,
                            np.abs(b['abc020'] - b['abc024']), 0)  # B2) NET DECREASE OF CASH FLOW FROM INVESTMENT ACTIVITIES (024-020)
        b['abc037'] = np.where((b['abc030'] - b['abc036']) >= 0,
                            np.abs(b['abc030'] - b['abc036']), 0)  # B2) NET DECREASE OF CASH FLOW FROM INVESTMENT ACTIVITIES (020-024)
        b['abc038'] = np.where((b['abc030'] - b['abc036']) <= 0,
                            np.abs(b['abc030'] - b['abc036']), 0)  # B2) NET DECREASE OF CASH FLOW FROM INVESTMENT ACTIVITIES (024-020)
        
        b['abc039'] = b['abc013'] - b['abc014'] + b['abc025'] - b['abc026'] +\
            b['abc037'] - b['abc038']  # Total increase of cash flow (013 – 014 + 025 – 026 + 037 – 038)
        b['abc040'] = b['abc014'] - b['abc013'] + b['abc026'] - b['abc025'] +\
            b['abc038'] - b['abc037']  # Total increase of cash flow (013 – 014 + 025 – 026 + 037 – 038)
        
    ntdSkupa = [tbl.loc[:, ~tbl.columns.str.startswith('b')] for tbl in ntdSkupa]
    ntdSkupa = pd.concat(ntdSkupa, axis=0)
    ntdSkupa.columns = ntdSkupa.columns.str.replace(r"abc", "b")
    ntdSkupa[['format_subjekta']] = "firma_2010"
    ntd = pd.concat(ntd, axis=0)
    ntd = pd.concat([ntd, ntdSkupa], axis=0)
    
    # firma 210 ZAVRSITI AKO SE POJAVI IZVJESTAJ S NOVCANIM TOKOM U KOJEM JE 2010 GODINA; ZA SADA IH NEMA
    # U R-U POCINJE U RETKU 641 (AKO NISAM NEŠTO U MEDJUVREMENU MJENJAO)
    # nt_skupa_2010  =[b.copy() for b in ntd if (b['format_subjekta'] == 'firma_2010').any()]

    ntd = pd.concat(ntd, axis=0)
    ntd = pd.concat([ntd, nt_skupa_2018], axis=0)

    return ntd


# MAIN
if __name__ == '__main__':
    
    dtime, link = get_fin_stat_links()
    
    # # stop script if there are no new financial statements
    # if not any(dtime.date == date.today()):
    #     print(' There is no new financial statements today')
    #     exit()
        
    gfi_metadata = get_metadata_from_links(link)

    # get current gfi metadata links from db
    urls = query_to_db('SELECT link FROM gfi_metadata', 'odvjet12_stocks')  # "SELECT * FROM gfi_metadata WhERE DATE(datetime) IN ('" + dayAndYesterday + "')"
    urls = urls['link'].to_list()
    
    # # stop script if urls already exists on db (no new financial statements)
    # if all([metadta in urls for metadta in gfi_metadata['link']]):
    #     print(" There is no new financial statements. ")
    #     exit()
    
    gfi_metadata = clean_gfi_table(gfi_metadata)
    
    # add gfiMeta to database
    write_to_db_update(gfi_metadata, 'odvjet12_stocks', 'gfi_metadata')
    
    # download new xls files
    # ############################## WHERE TO SAVE ? ###############################

    for i in range(len(gfi_metadata.index)):
        res = requests.get(gfi_metadata['link'][i])
        with open("./zse_fs/" + gfi_metadata['filename'][i], 'wb') as fs:  # WHERE TO SAVE
            fs.write(res.content)

    # ############################## WHERE TO SAVE ? ###############################

    # clean all financial statements
    bilanca = clean_balance_sheet(gfi_metadata)
    pl = clean_pl(gfi_metadata)
    nt = clean_nt(gfi_metadata)
    ntd = clean_ntd(gfi_metadata)

    # convert 2018 FS to 2010 FS
    bilanca = convert_2018_to_2010_bilanca(bilanca)
    pl = convert_2018_to_2010_rdg(pl)
    ntd = convert_2018_to_2010_ntd(ntd)



nt_skupa_2018 = [b.copy() for b in ntd if (b['format_subjekta'] == 'firma_2018').any()]
for b in nt_skupa_2018:
    b['abc001'] = b['b001']  # 1. Profit before tax
    b['abc002'] = b['b003']  # 2. Depreciation and amortisation
    b['abc003'] = np.where(b['b013'] >= 0, np.abs(b['b013']), 0)  # 3. Increase in short-term liabilities
    b['abc004'] = np.where(b['b014'] >= 0, np.abs(b['b014']), 0)  # 4. Decrease in short-term receivables
    b['abc005'] = np.where(b['b015'] >= 0, np.abs(b['b015']), 0)  # 5. Decrease in inventories
    b['abc006'] = np.where(b['b016'] >= 0, np.abs(b['b016']), 0) + \
        np.where((b['b011'] - b['b001'] - b['b003']) >= 0,
                np.abs(b['b011'] - b['b001'] - b['b003']), 0) + \
        np.where(b['b018'] >= 0, np.abs(b['b018']), 0) + \
        np.where(b['b019'] >= 0, np.abs(b['b019']), 0)   # 6. Other cash flow increases
    b['abc007'] = b['b001'] + b['b003'] + b['abc003'] + b['abc004'] +\
        b['abc005'] + b['abc006']
    b['abc008'] = np.where(b['b013'] <= 0, np.abs(b['b013']), 0)  # 1. Decrease in short-term liabilities
    b['abc009'] = np.where(b['b014'] <= 0, np.abs(b['b014']), 0)  # 2. Increase in short-term receivables
    b['abc010'] = np.where(b['b015'] <= 0, np.abs(b['b015']), 0)  # 3. Increase in inventories
    b['abc011'] = np.where(b['b016'] <= 0, np.abs(b['b016']), 0) + \
        np.where((b['b011'] - b['b001'] - b['b003']) <= 0,
                np.abs(b['b011'] - b['b001'] - b['b003']), 0) + \
        np.where(b['b018'] <= 0, np.abs(b['b018']), 0) + \
        np.where(b['b019'] <= 0, np.abs(b['b019']), 0)   # 6. Other cash flow increases
    b['abc012'] = b['b008'] + b['b009'] + b['abc010'] + b['abc011']

    b['abc015'] = np.abs(b['b021'])  # 1. Novčani primici od prodaje dugotrajne materijalne i nematerijalne imovine
    b['abc016'] = np.abs(b['b022'])  # 2. Cash inflows from sale of equity and debt instruments
    b['abc017'] = np.abs(b['b023'])  # 3. Interest receipts
    b['abc018'] = np.abs(b['b024'])  # 4. Dividends receipts
    b['abc019'] = b['b025'] + b['b026']  # 5. Other cash inflows from investment activities
    b['abc020'] = np.abs(b['b027'])  # III. Total cash inflows from investment activities (015 till 019)
    b['abc021'] = np.abs(b['b028'])  # 1. Cash outflows for purchase of long-term tangible and intangible assets
    b['abc022'] = np.abs(b['b029'])  # 2. Cash outflows for purchase of equity and debt financial instruments
    b['abc023'] = np.abs(b['b030'] + b['b031'] + b['b032'])  # 3. Other cash outflows from investment activities
    b['abc024'] = np.abs(b['b033'])  # IV. Total cash outflows from investment activities (021 till 023)
    
    b['abc027'] = b['b035'] + b['b036']  # 1. Cash receipts from issuance from equity and debt financial instruments
    b['abc028'] = np.abs(b['b037'])   # 2. Cash inflows from loans, debentures, credits and other borrowings
    b['abc029'] = np.abs(b['b038'])  # 3. Other cash inflows from financial activities
    b['abc030'] = np.abs(b['b039'])  # V. Total cash inflows from financial activities (027 till 029)
    b['abc031'] = np.abs(b['b040'])  # 1. Cash outflows for repayment of loans and bonds
    b['abc032'] = np.abs(b['b041'])  # 2. Dividends paid
    b['abc033'] = np.abs(b['b042'])  # 3. Cash outflows for finance lease
    b['abc034'] = np.abs(b['b043'])  # 4. Cash outflows for purchase of own stocks
    b['abc035'] = np.abs(b['b044'])  # 5. Other cash outflows from financial activities
    b['abc036'] = np.abs(b['b045'])  # VI. Total cash outflows from financial activities (031 till 035)
    
    b['abc041'] = np.abs(b['b049'])  # Cash and cash equivalents at the beginning of the period
    b['abc042'] = np.where(b['b048'] >= 0, b['b048'], 0)  # Increase of cash and cash equivalents
    b['abc043'] = np.where(b['b048'] <= 0, np.abs(b['b048']), 0)   # Decrease of cash and cash equivalents
    b['abc044'] = np.abs(b['b050'])  # Cash and cash equivalents at the end of the period
    
    b['abc013'] = np.where((b['abc007'] - b['abc012']) >= 0,
                        np.abs(b['abc007'] - b['abc012']), 0)  # A1) NET INCREASE OF CASH FLOW FROM OPERATING ACTIVITIES
    b['abc014'] = np.where((b['abc007'] - b['abc012']) <= 0,
                        np.abs(b['abc007'] - b['abc012']), 0)  # A2) NET DECREASE OF CASH FLOW FROM OPERATING ACTIVITIES (012-007)
    b['abc025'] = np.where((b['abc020'] - b['abc024']) >= 0,
                        np.abs(b['abc010'] - b['abc024']), 0)  # B1) NET INCREASE OF CASH FLOW FROM INVESTMENT ACTIVITIES (020-024)
    b['abc026'] = np.where((b['abc020'] - b['abc024']) <= 0,
                        np.abs(b['abc020'] - b['abc024']), 0)  # B2) NET DECREASE OF CASH FLOW FROM INVESTMENT ACTIVITIES (024-020)
    b['abc037'] = np.where((b['abc030'] - b['abc036']) >= 0,
                        np.abs(b['abc030'] - b['abc036']), 0)  # B2) NET DECREASE OF CASH FLOW FROM INVESTMENT ACTIVITIES (020-024)
    b['abc038'] = np.where((b['abc030'] - b['abc036']) <= 0,
                        np.abs(b['abc030'] - b['abc036']), 0)  # B2) NET DECREASE OF CASH FLOW FROM INVESTMENT ACTIVITIES (024-020)
    
    b['abc039'] = b['abc013'] - b['abc014'] + b['abc025'] - b['abc026'] +\
        b['abc037'] - b['abc038']  # Total increase of cash flow (013 – 014 + 025 – 026 + 037 – 038)
    b['abc040'] = b['abc014'] - b['abc013'] + b['abc026'] - b['abc025'] +\
        b['abc038'] - b['abc037']  # Total increase of cash flow (013 – 014 + 025 – 026 + 037 – 038)
    
nt_skupa_2018 = [tbl.loc[:, ~tbl.columns.str.startswith('b')] for tbl in nt_skupa_2018]
nt_skupa_2018 = pd.concat(nt_skupa_2018, axis=0)
nt_skupa_2018.columns = nt_skupa_2018.columns.str.replace(r"abc", "b")
nt_skupa_2018[['format_subjekta']] = "firma_2010"
ntd = pd.concat(ntd, axis=0)
ntd = pd.concat([ntd, nt_skupa_2018], axis=0)





ntd = clean_scraped_excel_tables(metadata, 'nt')
ntd = [n for n in ntd if n is not 'fond']
ntd = [n for n in ntd if n.shape[1] is not 37]

# add reports type
for n in ntd:
    aopLength = len(n.columns.str.extract(r'(b\d+)').dropna())
    if aopLength == 50:
        n['format_subjekta'] = "firma_2018"
    elif aopLength == 44:
        n['format_subjekta'] = "firma_2010"
    elif aopLength == 42:
        n['format_subjekta'] = "firma_2018_HHLD"
    elif aopLength == 42:
        n['format_subjekta'] = "firma_2010_NEPOZNATO"
    elif aopLength == 51:
        n['format_subjekta'] = "banka_2018"
    elif aopLength == 44:
        n['format_subjekta'] = "banka_2010"
    elif aopLength == 57 and n['report_year'] == 2019:
        n['format_subjekta'] = "osiguranje_2018"
    elif aopLength == 57:
        n['format_subjekta'] = "osiguranje_2010"
    elif aopLength == 40:
        n['format_subjekta'] = "zb_2010"
            

# NT
nt_process <- gfi_scrap(metadata, tip_gfi = "nt")
nt <- discard(nt_process, is.null)
if (length(which(nt == "fond")) != 0) {
  nt <- nt[-which(nt == "fond")]
}
# nt <- nt[-which(nt == "liste")]
nt <- map(nt, ~ dplyr::mutate(.x, report_year = max(year)))
nt <- lapply(nt, function(x) {
  if (("b015" %in% colnames(x)[1])) {
    x[, paste0("b", str_pad(1:14, 3, "left", "0"))] <- 0
  }
  x
})

# dodati tipove izvjestaja
nt <- lapply(nt, function(x) {
  x <- x %>% mutate(
    format_subjekta = case_when(
      (length(.) - length(grep("^([^0-9]*)$", colnames(.)))) == 50 ~ "firma_2018", 
      (length(.) - length(grep("^([^0-9]*)$", colnames(.)))) == 44 & colnames(.)[1] == "b073" ~ "firma_2009",
      (length(.) - length(grep("^([^0-9]*)$", colnames(.)))) == 44 ~ "firma_2010",
      (length(.) - length(grep("^([^0-9]*)$", colnames(.)))) == 45 ~ "firma_nepoznato",
      (length(.) - length(grep("^([^0-9]*)$", colnames(.)))) == 46 ~ "firma_2009_direktna_metoda",
      # (length(.) - 8) == 47 ~ "firma_2009_det",
      # (length(.) - length(grep("^([^0-9]*)$", colnames(.)))) == 51 ~ "banka_2018", # nema nijedne, dodat iz direktne metode
      (length(.) - length(grep("^([^0-9]*)$", colnames(.)))) == 38 & colnames(.)[1] == "b075" ~ "banka_2009",
      (length(.) - length(grep("^([^0-9]*)$", colnames(.)))) == 42 ~ "banka_2009_det",
      
      (length(.) - length(grep("^([^0-9]*)$", colnames(.)))) == 57 & report_year == 2019 ~ "osiguranje_2018",
      (length(.) - length(grep("^([^0-9]*)$", colnames(.)))) == 57 & report_year == 2018 & quarter == "1Y" ~ "osiguranje_2018",
      
      (length(.) - length(grep("^([^0-9]*)$", colnames(.)))) == 57 ~ "osiguranje_2010",
      (length(.) - length(grep("^([^0-9]*)$", colnames(.)))) == 59 ~ "osiguranje_2009",
      (length(.) - length(grep("^([^0-9]*)$", colnames(.)))) == 38 ~ "zb_2018"
    )
  )
  x
})
