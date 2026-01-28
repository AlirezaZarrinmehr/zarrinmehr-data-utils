#!/usr/bin/env python
# coding: utf-8

# # Zarrinmehr Data Utilities — Python Toolkit for Data Integration and ETL

# A curated collection of Python utility functions for data engineers and analysts.
# 

# In[1]:


'''
clean_df(
clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name,

read_csv_from_s3(
read_csv_from_s3(s3_client = s3_client, 

upload_to_s3(
upload_to_s3(s3_client = s3_client, data = 

fetch_data_from_timestream(query
fetch_data_from_timestream(timestream_query_client, query

'''


# ## Functions

# In[2]:


import importlib
modules = [
    "os",
    "time",
    "warnings",
    "io",
    "csv",
    "re",
    "json",
    "sys",
    "ftplib",
    "ast",
    "winsound",
    "base64",
    "inspect",
    "requests",
    "boto3",
    "pytz",
    "pyodbc",
    "psycopg2",
    "pandas_gbq",
    "importlib.util",
    "botocore.exceptions",
    "gc"
]
for mod in modules:
    try:
        globals()[mod] = importlib.import_module(mod)
    except ImportError as e:
        print(f"[INFO] Failed to import {mod}: {str(e)}")

modules = [
    ("datetime" , "date"),
    ("datetime" , "timedelta"),
    ("datetime" , "datetime"),
    ("requests_oauthlib" , "OAuth1"),
    ("tqdm" , "tqdm"),
    ("googleapiclient.discovery" , "build"),
    ("googleapiclient.errors" , "HttpError"),
    ("google.oauth2" , "service_account"),
    ("google.cloud" , "bigquery"),
    ("requests.auth" , "HTTPBasicAuth"),
    ("webdriver_manager.chrome" , "ChromeDriverManager"),
    ("selenium.webdriver.common.by" , "By"),
    ("selenium.webdriver.common.keys" , "Keys"),
    ("selenium.webdriver.support.ui" , "WebDriverWait"),
    ("bs4" , "BeautifulSoup"),
    ("sklearn.feature_extraction.text" , "TfidfVectorizer"),
    ("sklearn.multioutput" , "MultiOutputClassifier"),
    ("sklearn.ensemble" , "RandomForestClassifier"),
    ("sklearn.preprocessing" , "LabelEncoder"),
    ("sklearn.model_selection" , "train_test_split"),
    ("sklearn.metrics" , "accuracy_score"),
    ("psycopg2.errors", "DuplicateObject"),
    ("psycopg2.errors", "UndefinedTable"),
    ("itertools", "islice")
]
for fr_mod, im_mod in modules:
    try:
        mod = importlib.import_module(fr_mod)
        obj = getattr(mod, im_mod)
        globals()[im_mod] = obj
    except ImportError as e:
        print(f"[INFO] Failed to import {im_mod} from {fr_mod}: {str(e)}")

modules = {
    "pandas":  "pd",
    "numpy":  "np",
    "selenium.webdriver.support.expected_conditions": "EC",
    "selenium.webdriver": "webdriver"
}
for mod, alias in modules.items():
    try:
        globals()[alias] = importlib.import_module(mod)
    except ImportError as e:
        print(f"[INFO] Failed to import {mod} as {alias}: {str(e)}")

modules = [
    ("selenium.webdriver.chrome.service" , "Service", "ChromeService"),
]
for fr_mod, im_mod, name in modules:
    try:
        fr_mod = importlib.import_module(fr_mod)
        obj = getattr(fr_mod, im_mod)
        globals()[name] = obj
    except ImportError as e:
        print(f"[INFO] Failed to import {im_mod} from {fr_mod}: {str(e)}")

caller_globals = inspect.stack()[1][0].f_globals
for name in list(globals()):
    if not name.startswith("_") and name not in ['caller_globals', 'inspect']:
        caller_globals[name] = globals()[name]


def truncate_with_etc_1(s, truncate_len):
    return s[:truncate_len - 5] + ' etc.' if len(s) > truncate_len else s

def truncate_with_etc_2(s, truncate_len):

    byte_len = len(s.encode('utf-8'))
    if byte_len > truncate_len:
        truncated_str = s.encode('utf-8')[:truncate_len - 5]
        truncated_str = truncated_str.decode('utf-8', errors='ignore')
        return truncated_str + ' etc.'
    else:
        return s

def kill_qb_processes():
    processes = ["QBW.EXE", "axlbridge.exe"]
    for proc in processes:
        os.system(f"taskkill /f /im {proc} /t 2>nul")
    print("Cleaned up lingering QB/QODBC processes.")


def process_gp_transactions(
    list_of_accounts,
    companyName,
    salesOrderInvoiceHeader,
    salesOrderInvoiceLine,
    account,
    CadUsdAvg,
    item,
    customer,
    start_date,
    end_date,
    s3_client,
    s3_bucket_name
):
    generalLedgerLineOpen = read_csv_from_s3(s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'GL20000 :: YEAR-TO-DATE TRANSACTION OPEN.csv')
    generalLedgerLineHistory = read_csv_from_s3(s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'GL30000 :: ACCOUNT TRANSACTION HISTORY.csv')
    generalLedgerLine = pd.concat([generalLedgerLineOpen, generalLedgerLineHistory], ignore_index=True)
    sourceDocumentMaster = read_csv_from_s3(s3_client = s3_client, bucket_name=s3_bucket_name, object_key='SY00900 :: SOURCE DOCUMENT MASTER.csv')
    generalLedgerLine['SOURCDOC'] = generalLedgerLine['SOURCDOC'].astype('str').str.strip()
    sourceDocumentMaster['SOURCDOC'] = sourceDocumentMaster['SOURCDOC'].astype('str').str.strip()
    generalLedgerLine = generalLedgerLine.merge(sourceDocumentMaster[['SOURCDOC', 'SDOCDSCR']], on = 'SOURCDOC', how = 'left')
    generalLedgerLine['SDOCDSCR']=generalLedgerLine['SDOCDSCR'].map({'BBF':'Balance Brought Forward'})
    generalLedgerLine = generalLedgerLine.loc[
        (generalLedgerLine['ACTINDX'].astype('str').str.strip().isin(list_of_accounts['ACTINDX'].astype('str').astype('str').str.strip()))&\
        (pd.to_datetime(generalLedgerLine['TRXDATE'], errors='coerce')>=start_date)&\
        (pd.to_datetime(generalLedgerLine['TRXDATE'], errors='coerce')<=end_date)
    ].copy()
    generalLedgerLine.rename(columns = {'ORDOCNUM':'SOPNUMBE'}, inplace = True)
    generalLedgerLine["SOPNUMBE"] = generalLedgerLine["SOPNUMBE"].astype('str').str.strip()
    generalLedgerLine["ACTINDX"] = generalLedgerLine["ACTINDX"].astype('str').str.strip()
    generalLedgerLine['CRDTAMNT'] = generalLedgerLine['CRDTAMNT'].astype('float')
    generalLedgerLine['DEBITAMT'] = generalLedgerLine['DEBITAMT'].astype('float')
    generalLedgerLine['Total'] = generalLedgerLine['CRDTAMNT'] - generalLedgerLine['DEBITAMT']
    generalLedgerLine['TransactionId']='GJ' + ' :: ' + generalLedgerLine['JRNENTRY'].astype('str').str.strip() + ' :: ' + generalLedgerLine['TRXSORCE'].astype('str').str.strip()
    generalLedgerLine['TransactionNo']='GJ' + ' :: ' + generalLedgerLine['JRNENTRY'].astype('str').str.strip()
    generalLedgerLine['TransactionStatus'] = 'CLOSED'
    generalLedgerLine['TransactionType'] = 'GENERAL JOURNAL'
    generalLedgerLine['TransactionDate'] = pd.to_datetime(generalLedgerLine['TRXDATE'])
    generalLedgerLine['ItemId'] = 'GENERAL JOURNAL'
    generalLedgerLine['ItemDescription']='GENERAL JOURNAL' + ' :: ' + generalLedgerLine['REFRENCE']
    generalLedgerLine['Quantity'] = 1
    generalLedgerLine['Rate'] = generalLedgerLine['Total']
    generalLedgerLine.rename(columns={'ORMSTRID':'CustId'}, inplace = True)
    txns = salesOrderInvoiceHeader.loc[
        (salesOrderInvoiceHeader['SOPTYPE']=='Invoice')&\
        (pd.to_datetime(salesOrderInvoiceHeader['GLPOSTDT'], errors='coerce')<=end_date)&\
        (pd.to_datetime(salesOrderInvoiceHeader['GLPOSTDT'], errors='coerce')<=end_date)
    ].copy()
    txns["SOPNUMBE"] = txns["SOPNUMBE"].astype('str').str.strip()   
    txnsLines = salesOrderInvoiceLine.loc[
        (salesOrderInvoiceLine['SOPTYPE']=='Invoice')&\
        (salesOrderInvoiceLine['SLSINDX'].astype('str').str.strip().isin(list_of_accounts['ACTINDX'].astype('str').str.strip()))
    ].copy()
    txnsLines.rename(columns = {'SLSINDX':'ACTINDX'}, inplace = True)
    txnsLines["SOPNUMBE"] = txnsLines["SOPNUMBE"].astype('str').str.strip()
    txnsLines["ACTINDX"] = txnsLines["ACTINDX"].astype('str').str.strip()
    txnsLines['XTNDPRCE'] = txnsLines['XTNDPRCE'].astype('float')
    txnsLines = txnsLines.merge(txns, on = ["SOPNUMBE"], suffixes = ('', '_inv'))
    txnsLines.rename(columns = {
        'ORIGNUMB':'OrderId',
        'GLPOSTDT':'TransactionDate',
        'SOPSTATUS':'TransactionStatus',
        'SLPRSNID':'SalesRepID',
        'CUSTNMBR':'CustId',
        'CSTPONBR':'CustPo',
        'FUFILDAT':'CloseDate',
        'ShipToName':'ShipName',
        'CITY':'ShipCity',
        'STATE':'ShipState',
        'ZIPCODE':'ShipZip',
        'SUBTOTAL':'HeaderTotal',
        'ITEMNMBR':'ItemId',
        'ACTLSHIP':'ShipDate',
        'XTNDPRCE':'Total'
    }, inplace = True)
    txnsLines['TransactionType']='INVOICE'
    txnsLines['TransactionId']=txnsLines['SOPNUMBE']
    txnsLines['TransactionNo']=txnsLines['SOPNUMBE']
    txnsLines['TransactionDate'] = pd.to_datetime(txnsLines['TransactionDate'])
    txnsLines['Quantity']=txnsLines['QUANTITY']
    txnsLines['Rate']=txnsLines['UNITPRCE']
    txnsLines['ItemDescription']=txnsLines['ITEMDESC']
    matchedGlTranLin= pd.merge(
        generalLedgerLine.groupby('SOPNUMBE').agg({'Total':'sum'}).reset_index(),
        txnsLines.groupby('SOPNUMBE').agg({'Total':'sum'}).reset_index(),
        on = 'SOPNUMBE',
    )
    matchedGlTranLin = matchedGlTranLin[np.isclose(matchedGlTranLin['Total_x'], matchedGlTranLin['Total_y'], atol=0.01)]
    txns = txns.loc[
    (txns['SOPNUMBE'].isin(matchedGlTranLin['SOPNUMBE']))
    ]
    txnsLines = txnsLines.loc[
    (txnsLines['SOPNUMBE'].isin(matchedGlTranLin['SOPNUMBE']))
    ]
    generalLedgerLine = generalLedgerLine.loc[~generalLedgerLine['SOPNUMBE'].isin(matchedGlTranLin['SOPNUMBE'])]
    txnsLines = pd.concat([txnsLines, generalLedgerLine], ignore_index=True)
    txnsLines = txnsLines.merge(account[['ACTINDX', 'ACTDESCR']].rename(columns = {'ACTDESCR':'Account'}), on ='ACTINDX', how = 'left')
    txnsLines['CustId'] = txnsLines['CustId'].astype('str').str.strip()
    customer['CustId'] = customer['CustId'].astype('str').str.strip()
    txnsLines = txnsLines.merge(customer[['CustId', 'CustNo', 'CustName']], on = 'CustId', how = 'left')
    txnsLines['ItemId'] = txnsLines['ItemId'].astype('str').str.strip()
    item['ItemId'] = item['ItemId'].astype('str').str.strip()
    txnsLines = txnsLines.merge(item[['ItemId', 'ItemNo', 'ItemName']], on='ItemId', how = 'left', suffixes = ('', '_Item'))
    billToAdds = customer.rename(columns={'CustName':'BillName', 'CustCity':'BillCity', 'CustState':'BillState', 'CustZip':'BillZip'})[['CustId', 'BillName', 'BillCity', 'BillState', 'BillZip']]
    billToAdds['CustId'] = billToAdds['CustId'].astype('str').str.strip()
    customer['CustId'] = customer['CustId'].astype('str').str.strip()
    txnsLines = txnsLines.merge(billToAdds, on = 'CustId', how = 'left')
    txnsLines = txnsLines.merge(CadUsdAvg, left_on=[txnsLines['TransactionDate'].dt.year, txnsLines['TransactionDate'].dt.month], right_on=['Year', 'Month'], how = 'left')
    txnsLines['CAD/USD'] = txnsLines['CAD/USD'].interpolate(method='linear', limit_direction='both')
    txnsLines['Rate'] = pd.to_numeric(txnsLines['Rate'], errors='coerce')
    txnsLines['Rate']=txnsLines['Rate']*txnsLines['CAD/USD']
    txnsLines['Total']=txnsLines['Total']*txnsLines['CAD/USD']
    txns = txnsLines.fillna('').groupby( 'TransactionId', as_index=False ).agg({'OrderId': 'max', 'CustId': 'max', 'TransactionStatus': 'max', 'TransactionNo': 'max', 'TransactionType': 'max', 'TransactionDate': 'max', 'SalesRepID': 'max', 'CustPo': 'max', 'CustNo': 'max', 'CustName': 'max', 'ShipName': 'max', 'ShipCity': 'max', 'ShipState': 'max', 'ShipZip': 'max', 'BillName': 'max', 'BillCity': 'max', 'BillState': 'max', 'BillZip': 'max', 'Total': 'sum'})
    txnsLines = txnsLines[['TransactionId', 'TransactionNo', 'Account', 'ItemId', 'ItemNo', 'ItemName', 'ItemDescription', 'Rate', 'Quantity', 'Total']]
    txns['Company'] = companyName
    txns = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns, df_name = 'txns', id_column = ['TransactionId'], additional_date_columns = [], zip_code_columns = ['BillZip'], state_columns = ['BillState'], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
    txns = txns[['Company'] + txns.columns[:-1].tolist()]
    txns.TransactionId = txns.TransactionId.astype('str').str.strip()
    txnsLines.TransactionId = txnsLines.TransactionId.astype('str').str.strip()
    mismatched_txns = txns.merge(txnsLines, on='TransactionId', how='inner', suffixes=('_ord', '_lin')).groupby('TransactionId').agg({'Total_ord':'max', 'Total_lin':'sum'}).reset_index()
    mismatched_txns = mismatched_txns[~np.isclose(mismatched_txns['Total_ord'], mismatched_txns['Total_lin'], atol=0.1)]
    print(f"{mismatched_txns.shape[0]} txns total do not match txnsline total")
    txns = txns[~txns['TransactionId'].isin(mismatched_txns['TransactionId'])]
    txnsLines['Company'] = companyName
    txnsLines = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txnsLines, df_name = 'txnsLines', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
    txnsLines = txnsLines[['Company'] + txnsLines.columns[:-1].tolist()]
    txnsLines = txnsLines[txnsLines['TransactionId'].isin(txns['TransactionId'])]
    return txns, txnsLines

def process_gp_orders(
    companyName,
    dfHeader,
    dfLine,
    item,
    item_df,
    customersORvendors,
    start_date,
    end_date,
    s3_client,
    s3_bucket_name,
    txnsType,
    txnsType2,
    txnsType3,
    txnsType4,
    txnsType5,
    txnsType6,
    txnsType7,
    txnsType8,
    txnsType9,
    txnsType10,
    txnsType11,
    txnsType12,
    txnsType13,
    txnsType14,
    txnsType15,
    txnsType16,
    txnsType17,
    CadUsdAvg,
    txns,
    DBIA,
    itemsCategoriesV3
):
    orders = dfHeader[
        (pd.to_datetime(dfHeader['DOCDATE'], errors='coerce')>=start_date)&\
        (pd.to_datetime(dfHeader['DOCDATE'], errors='coerce')<=end_date)&\
        (dfHeader[txnsType7].isin(txnsType17))
    ].copy()

    orders = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = orders, df_name = 'orders', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
    orders = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = orders, df_name = 'orders', id_column = [txnsType], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
    orders.rename(columns = {
        txnsType:f'{txnsType2}No', 
        'DOCDATE':f'{txnsType2}Date',
        txnsType6:f'{txnsType2}Status',
        'ACTLSHIP':'ShipDate',
        txnsType8:txnsType4,
        txnsType9:f'{txnsType3}Id',
        txnsType10:txnsType5,
        txnsType11:'CloseDate',
        txnsType12:'ShipName',
        'CITY':'ShipCity',
        'STATE':'ShipState',
        'ZIPCODE':'ShipZip',
        'SUBTOTAL':'Total',
    }, inplace = True)
    orders[f'{txnsType2}Id']=orders[f'{txnsType2}No']
    orders[f'{txnsType2}Status'] = orders[f'{txnsType2}Status'].astype('str')
    orders = orders.merge(customersORvendors[[f'{txnsType3}Id', f'{txnsType3}No', f'{txnsType3}Name']], on = f'{txnsType3}Id', how = 'left')
                
    ordersLines = dfLine[
        (dfLine[txnsType].isin(orders[f'{txnsType2}Id']))&\
        (dfLine[txnsType7].isin(txnsType17))
    ].copy()

    ordersLines.rename(columns = {
        txnsType:f'{txnsType2}No',
        'ITEMNMBR':'ItemId',
        'ITEMDESC':'ItemDescription',
        txnsType13:'Quantity',
        txnsType14:'Rate',
        txnsType15:'Total',
        txnsType16:'ShipDate'
    }, inplace = True)
    ordersLines[f'{txnsType2}Id']=ordersLines[f'{txnsType2}No']
    ordersLines = ordersLines.merge(orders[[f'{txnsType2}Id', f'{txnsType2}Date']], on = f'{txnsType2}Id', how ='left')
    ordersLines['Total'] = pd.to_numeric(ordersLines['Total'], errors='coerce')

    ordersLines['ItemId'] = ordersLines['ItemId'].astype('str').str.strip()
    item['ItemId'] = item['ItemId'].astype('str').str.strip()
    ordersLines = ordersLines.merge(item[['ItemId', 'ItemNo', 'ItemName']], on='ItemId', how = 'left', suffixes = ('', '_Item'))
    ordersLines = ordersLines.merge(CadUsdAvg, left_on=[ordersLines[f'{txnsType2}Date'].dt.year, ordersLines[f'{txnsType2}Date'].dt.month], right_on=['Year', 'Month'], how = 'left')
    ordersLines['CAD/USD'] = ordersLines['CAD/USD'].interpolate(method='linear', limit_direction='both')
    ordersLines['Rate'] = pd.to_numeric(ordersLines['Rate'], errors='coerce')
    ordersLines['Rate']=ordersLines['Rate']*ordersLines['CAD/USD']
    ordersLines['Total']=ordersLines['Total']*ordersLines['CAD/USD']
    ordersLines = ordersLines[[f'{txnsType2}Id', f'{txnsType2}No', 'ItemId', 'ItemNo', 'ItemName', 'ItemDescription', 'Quantity', 'Rate', 'Total', 'ShipDate']]
    orders = orders[[f'{txnsType2}Id', f'{txnsType2}No', f'{txnsType2}Status', f'{txnsType2}Date', 'CloseDate', txnsType4, txnsType5, f'{txnsType3}Id', f'{txnsType3}No', f'{txnsType3}Name', 'ShipName', 'ShipCity', 'ShipState', 'ShipZip', 'Total']].copy()
    orders.drop(columns =['Total'], inplace = True)
    totals = (
        ordersLines
        .groupby(f'{txnsType2}Id', as_index=False)['Total']
        .sum()
    )

    orders = orders.merge(totals, on=f'{txnsType2}Id', how='left')
    orders['Company'] = companyName
    orders = orders[['Company'] + orders.columns[:-1].tolist()]
    orders = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = orders, df_name = 'orders', id_column = [f'{txnsType2}Id'], additional_date_columns = [], zip_code_columns = ['ShipZip'], state_columns = ['ShipState'], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
    orders[f'{txnsType2}Id'] = orders[f'{txnsType2}Id'].fillna(0).apply(convert_to_int_or_keep).astype('str')
    ordersLines[f'{txnsType2}Id'] = ordersLines[f'{txnsType2}Id'].fillna(0).apply(convert_to_int_or_keep).astype('str')
    orders[f'{txnsType2}Id'] = orders[f'{txnsType2}Id'].astype(str)

    mismatched_orders = orders.merge(ordersLines, on=f'{txnsType2}Id', how='inner', suffixes=('_ord', '_lin')).groupby(f'{txnsType2}Id').agg({'Total_ord':'max', 'Total_lin':'sum'}).reset_index()
    mismatched_orders = mismatched_orders[~np.isclose(mismatched_orders['Total_ord'], mismatched_orders['Total_lin'], atol=0.1)]
    print(f"{mismatched_orders.shape[0]} orders total do not match orderline total")
    orders = orders[~orders[f'{txnsType2}Id'].isin(mismatched_orders[f'{txnsType2}Id'])]
    #-------------------------------------
    orders = orders.drop_duplicates(subset=[f'{txnsType2}Id'])
    orders = orders.loc[orders[f'{txnsType2}Id'].notna() & (orders[f'{txnsType2}Id'].astype('str').str.strip() != '')]
    #-------------------------------------
    ordersLines, itemsCategoriesV3, item_df = enrich_and_classify_items(
        item_df, 
        companyName, 
        s3_client, 
        s3_bucket_name, 
        DBIA, 
        itemsCategoriesV3,
        ordersLines
    )
    #-----------------------------------------------------------------------------------------------------------
    orderTypes = ordersLines.merge(
                        itemsCategoriesV3[['index', 'ItemLevel2']] \
                        .rename(columns = {'index':'ItemId'}) \
                        .drop_duplicates(subset = 'ItemId'), on = 'ItemId'
                    ) \
                    .rename(columns={'ItemLevel2': f'{txnsType2}Type'}) \
                    .sort_values([f'{txnsType2}Id', 'Total'], ascending=[True, False]) \
                    .groupby(f'{txnsType2}Id').agg({f'{txnsType2}Type': 'first'}).reset_index()

    #-------------------------------------

    ordersLines['Company'] = companyName
    ordersLines = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = ordersLines, df_name = 'ordersLines', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
    ordersLines = ordersLines[['Company'] + ordersLines.columns[:-1].tolist()]
    ordersLines = ordersLines[ordersLines[f'{txnsType2}Id'].isin(orders[f'{txnsType2}Id'])]
    #-----------------------------------------------------------------------------------------------------------
    ordersLines['InstallDate'] = np.nan
    #-----------------------------------------------------------------------------------------------------------
    ordersLines.loc[
        (pd.to_datetime(ordersLines['ShipDate']) < start_date) |
        (pd.to_datetime(ordersLines['ShipDate']) > end_date),
        'ShipDate'
    ] = np.nan
    ordersLines.loc[
        (pd.to_datetime(ordersLines['InstallDate']) < start_date) |
        (pd.to_datetime(ordersLines['InstallDate']) > end_date),
        'InstallDate'
    ] = np.nan
    #-----------------------------------------------------------------------------------------------------------
    txns[f'{txnsType2}Id'] = txns[f'{txnsType2}Id'].fillna(0).apply(convert_to_int_or_keep).astype(str)
    ordersLines[f'{txnsType2}Id'] = ordersLines[f'{txnsType2}Id'].fillna(0).apply(convert_to_int_or_keep).astype(str)
    ordersLines = ordersLines.merge(txns[[f'{txnsType2}Id', 'TransactionDate']].dropna().drop_duplicates(subset=[f'{txnsType2}Id']).rename(columns={'TransactionDate':'InvoiceDate'}), on = f'{txnsType2}Id', how = 'left')
    #-----------------------------------------------------------------------------------------------------------
    orders[f'{txnsType2}Id'] = orders[f'{txnsType2}Id'].fillna(0).apply(convert_to_int_or_keep).astype(str)
    ordersLines[f'{txnsType2}Id'] = ordersLines[f'{txnsType2}Id'].fillna(0).apply(convert_to_int_or_keep).astype(str)
    ordersLines = ordersLines.merge(orders[[f'{txnsType2}Id', f'{txnsType2}Status']], on = f'{txnsType2}Id', how = 'left').rename(columns={f'{txnsType2}Status':'ItemStatus'})
    ordersLines.loc[(ordersLines['ShipDate'].notna()), 'ItemStatus'] = 'SHIPPED'
    ordersLines.loc[(ordersLines['InstallDate'].notna()), 'ItemStatus'] = 'INSTALLED'
    ordersLines.loc[(ordersLines['InvoiceDate'].notna()), 'ItemStatus'] = 'INVOICED'
    #-----------------------------------------------------------------------------------------------------------
    ordersLines.rename(columns = {'CommonName':'ItemType'}, inplace = True)
    #-----------------------------------------------------------------------------------------------------------
    return orders, ordersLines, item_df


def process_qb_expense_transactions(
    list_of_accounts,
    companyName,
    transactions,
    item,
    vendors,
    start_date,
    end_date,
    s3_client,
    s3_bucket_name
):
    transactions = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'Transaction.csv', is_csv_file=True )

    generalJournal = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'JournalEntry.csv', is_csv_file=True )
    generalJournalLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'JournalEntryLine.csv', is_csv_file=True )
    generalJournalLines = generalJournal.merge(generalJournalLines, on = 'Txnid', suffixes = ('_h', ''))
    generalJournalLines = generalJournalLines.reset_index(drop=True).reset_index()
    generalJournalLines['index']='GENERAL JOURNAL' + generalJournalLines['index'].astype(str)
    generalJournalLines.rename(columns = {
        'index':'TransactionId',
        'Txndate':'TransactionDate',
        'Refnumber':'TransactionNo',
        'Journallinetype':'PurchaseOrderNo',
        'Txndate':'TransactionDate',
        'Journallineentityreffullname':'VendNo',
        'Journallineaccountreffullname':'Account',
        'Journallineclassreffullname':'ItemId',
        'Journallinememo':'ItemDescription',
        'Itemlinequantity':'Quantity',
        'Itemlinecost':'Rate',
        'Journallineamount':'Total'
    }, inplace = True)
   
    generalJournalLines['Total']=-generalJournalLines['Total']
    generalJournalLines['Quantity']=1
    generalJournalLines['Rate']=generalJournalLines['Total']
    generalJournal = generalJournalLines [['TransactionId','TransactionNo','PurchaseOrderNo','TransactionDate','VendNo','Total']].copy()
    generalJournal['TransactionType'] = 'GENERAL JOURNAL'
    generalJournalLines = generalJournalLines [['TransactionId','TransactionDate','TransactionNo','Account','ItemId','ItemDescription','Quantity','Rate','Total']]

    bills = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'Bill.csv', is_csv_file=True )
    bills.rename(columns = {
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Memo':'PurchaseOrderNo',
        'Txndate':'TransactionDate',
        'Vendorreffullname':'VendNo',
        'Vendoraddressaddr1':'BillName',
        'Vendoraddresscity':'BillCity',
        'Vendoraddressstate':'BillState',
        'Vendoraddresspostalcode':'BillZip',
        'Amountdue':'Total',        
    }, inplace = True)
    bills['TransactionType'] = 'BILL'
    billsLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'BillItemLine.csv', is_csv_file=True )
    billsLines=billsLines.merge(transactions[['Fqtxnlinkkey', 'Accountreflistid', 'Accountreffullname']], on = 'Fqtxnlinkkey', how = 'left')
    billsLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Itemlineamount':'Total'
    }, inplace = True)
    billsLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Accountreffullname':'Account',
        'Itemlineitemreffullname':'ItemId',
        'Itemlinedesc':'ItemDescription',
        'Itemlinequantity':'Quantity',
        'Itemlinecost':'Rate',
    }, inplace = True)
    billExpenseLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'BillExpenseLine.csv', is_csv_file=True )
    billExpenseLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Expenselineaccountreffullname':'Accountreffullname',
        'Expenselineamount':'Total'
    }, inplace = True)
    billExpenseLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Accountreffullname':'Account',
        'Itemlineitemreffullname':'ItemId',
        'Itemlinedesc':'ItemDescription',
        'Itemlinequantity':'Quantity',
        'Itemlinecost':'Rate',
    }, inplace = True)
    vendorCredit = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'VendorCredit.csv', is_csv_file=True )
    vendorCredit.rename(columns = {
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Memo':'PurchaseOrderNo',
        'Txndate':'TransactionDate',
        'Vendorreffullname':'VendNo',
        'Vendoraddressaddr1':'BillName',
        'Vendoraddresscity':'BillCity',
        'Vendoraddressstate':'BillState',
        'Vendoraddresspostalcode':'BillZip',
        'Amountdue':'Total',        
    }, inplace = True)
    vendorCredit['TransactionType'] = 'VENDOR CREDIT'
    vendorCreditLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'VendorCreditItemLine.csv', is_csv_file=True )
    vendorCreditLines=vendorCreditLines.merge(transactions[['Fqtxnlinkkey', 'Accountreflistid', 'Accountreffullname']], on = 'Fqtxnlinkkey', how = 'left')
    vendorCreditLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Itemlineamount':'Total'
    }, inplace = True)
    vendorCreditLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Accountreffullname':'Account',
        'Itemlineitemreffullname':'ItemId',
        'Itemlinedesc':'ItemDescription',
        'Itemlinequantity':'Quantity',
        'Itemlinecost':'Rate',
    }, inplace = True)
    vendorCreditLines['Total']=-vendorCreditLines['Total']
    vendorCreditExpenseLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'VendorCreditExpenseLine.csv', is_csv_file=True )
    vendorCreditExpenseLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Expenselineaccountreffullname':'Accountreffullname',
        'Expenselineamount':'Total'
    }, inplace = True)
    vendorCreditExpenseLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Accountreffullname':'Account',
        'Itemlineitemreffullname':'ItemId',
        'Itemlinedesc':'ItemDescription',
        'Itemlinequantity':'Quantity',
        'Itemlinecost':'Rate',
    }, inplace = True)
    vendorCreditExpenseLines['Total']=-vendorCreditExpenseLines['Total']

    checks = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'Check.csv', is_csv_file=True )
    checks.rename(columns = {
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Memo':'PurchaseOrderNo',
        'Txndate':'TransactionDate',
        'Payeeentityreffullname':'VendNo',
        'Addressaddr1':'BillName',
        'Addresscity':'BillCity',
        'Addressstate':'BillState',
        'Addresspostalcode':'BillZip',
        'Amount':'Total',          
    }, inplace = True)
    checks = checks [['TransactionId','TransactionNo','PurchaseOrderNo','TransactionDate','VendNo','BillName','BillCity','BillState','BillZip','Total']]
    checks['TransactionType'] = 'CHECK'
    checksLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'CheckItemLine.csv', is_csv_file=True )
    checksLines.drop(columns =['Accountreflistid', 'Accountreffullname'], inplace = True)
    checksLines=checksLines.merge(transactions[['Fqtxnlinkkey', 'Accountreflistid', 'Accountreffullname']], on = 'Fqtxnlinkkey', how = 'left')
    checksLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Accountreffullname':'Account',
        'Itemlineitemreffullname':'ItemId',
        'Itemlinedesc':'ItemDescription',
        'Itemlinequantity':'Quantity',
        'Itemlinecost':'Total'
    }, inplace = True)
    checksLines['Rate']=checksLines['Total']/checksLines['Quantity']
    checksLines = checksLines [['TransactionId','TransactionDate','TransactionNo','Account','ItemId','ItemDescription','Quantity','Rate','Total']]

    checkExpenseLine = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'CheckExpenseLine.csv', is_csv_file=True )
    checkExpenseLine.drop(columns =['Accountreflistid', 'Accountreffullname'], inplace = True)
    checkExpenseLine.rename(columns = {
        'Txndate':'TransactionDate',
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Expenselineaccountreffullname':'Account',
        'Expenselineclassreffullname':'ItemId',
        'Expenselinememo':'ItemDescription',
        'Expenselineamount':'Total'
    }, inplace = True)
    checkExpenseLine['Quantity']=1
    checkExpenseLine['Rate']=checkExpenseLine['Total']
    checkExpenseLine = checkExpenseLine [['TransactionId','TransactionDate','TransactionNo','Account','ItemId','ItemDescription','Quantity','Rate','Total']]

    salesReceipts = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'SalesReceipt.csv', is_csv_file=True )
    salesReceipts.rename(columns = {
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Classreffullname':'PurchaseOrderNo',
        'Txndate':'TransactionDate',
        'Customerreffullname':'VendNo',
        'Billaddressaddr1':'BillName',
        'Billaddresscity':'BillCity',
        'Billaddressstate':'BillState',
        'Billaddresspostalcode':'BillZip',
        'Totalamount':'Total',        
    }, inplace = True)
    salesReceipts = salesReceipts [['TransactionId','TransactionNo','PurchaseOrderNo','TransactionDate','VendNo','BillName','BillCity','BillState','BillZip','Total']]
    salesReceipts['TransactionType'] = 'SALES RECEIPT'
    salesReceiptsLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'SalesReceiptLine.csv', is_csv_file=True )
    salesReceiptsLines=salesReceiptsLines.merge(transactions[['Fqtxnlinkkey', 'Accountreflistid', 'Accountreffullname', 'Amount']], on = ['Fqtxnlinkkey'], how = 'left')
    salesReceiptsLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Accountreffullname':'Account',
        'Salesreceiptlineitemreffullname':'ItemId',
        'Salesreceiptlinedesc':'ItemDescription',
        'Salesreceiptlinequantity':'Quantity',
        'Salesreceiptlinerate':'Rate',
        'Amount':'Total'
    }, inplace = True)
    salesReceiptsLines = salesReceiptsLines [['TransactionId','TransactionDate','TransactionNo','Account','ItemId','ItemDescription','Quantity','Rate','Total']]

    receivePayment = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'ReceivePayment.csv', is_csv_file=True )
    receivePayment.rename(columns = {
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Memo':'PurchaseOrderNo',
        'Txndate':'TransactionDate',
        'Vendorreffullname':'VendNo',
        'Vendoraddressaddr1':'BillName',
        'Vendoraddresscity':'BillCity',
        'Vendoraddressstate':'BillState',
        'Vendoraddresspostalcode':'BillZip',
        'Amountdue':'Total',        
    }, inplace = True)
    receivePayment['TransactionType'] = 'RECEIVE PAYMENT'
    receivePaymentLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'ReceivePaymentLine.csv', is_csv_file=True )
    receivePaymentLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Appliedtotxndiscountaccountreffullname':'Accountreffullname',
        'Appliedtotxndiscountamount':'Total'
    }, inplace = True)
    receivePaymentLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Accountreffullname':'Account',
        'Itemlineitemreffullname':'ItemId',
        'Itemlinedesc':'ItemDescription',
        'Itemlinequantity':'Quantity',
        'Itemlinecost':'Rate',
    }, inplace = True)

    deposit = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'Deposit.csv', is_csv_file=True )
    depositLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'DepositLine.csv', is_csv_file=True )
    depositLines = deposit.merge(depositLines, on = 'Txnid', suffixes = ('_h', ''))
    depositLines = depositLines.reset_index(drop=True).reset_index()
    depositLines['index']='DEPOSIT' + depositLines['index'].astype(str)
    depositLines.rename(columns = {
        'index':'TransactionId',
        'Txndate':'TransactionDate',
        'Depositlinechecknumber':'TransactionNo',
        'Memo':'PurchaseOrderNo',
        'Txndate':'TransactionDate',
        'Depositlineentityreffullname':'VendNo',
        'Depositlineaccountreffullname':'Account',
        'Depositlineclassreffullname':'ItemId',
        'Depositlinememo':'ItemDescription',
        'Itemlinequantity':'Quantity',
        'Itemlinecost':'Rate',
        'Depositlineamount':'Total'
    }, inplace = True)
    depositLines['Total']=-depositLines['Total']
    depositLines['Quantity']=1
    depositLines['Rate']=depositLines['Total']
    deposit = depositLines [['TransactionId','TransactionNo','PurchaseOrderNo','TransactionDate','VendNo','Total']].copy()
    deposit['TransactionType'] = 'DEPOSIT'
    depositLines = depositLines [['TransactionId','TransactionDate','TransactionNo','Account','ItemId','ItemDescription','Quantity','Rate','Total']]

    billPaymentCheck = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'BillPaymentCheck.csv', is_csv_file=True )
    billPaymentCheck.rename(columns = {
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Memo':'PurchaseOrderNo',
        'Txndate':'TransactionDate',
        'Payeeentityreffullname':'VendNo',
        'Addressaddr1':'BillName',
        'Addresscity':'BillCity',
        'Addressstate':'BillState',
        'Addresspostalcode':'BillZip',
        'Amount':'Total',        
    }, inplace = True)
    billPaymentCheck = billPaymentCheck [['TransactionId','TransactionNo','PurchaseOrderNo','TransactionDate','VendNo','BillName','BillCity','BillState','BillZip','Total']]
    billPaymentCheck['TransactionType'] = 'BILL PAYMENT CHECK'
    billPaymentCheckLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'BillPaymentCheckLine.csv', is_csv_file=True )
    billPaymentCheckLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Appliedtotxndiscountaccountreffullname':'Account',
        'Appliedtotxnrefnumber':'ItemId',
        'Appliedtotxndiscountamount':'Total'
    }, inplace = True)
    billPaymentCheckLines['Total']=-billPaymentCheckLines['Total']
    billPaymentCheckLines['ItemDescription']=billPaymentCheckLines['ItemId']
    billPaymentCheckLines['Quantity']=1
    billPaymentCheckLines['Rate']=billPaymentCheckLines['Total']
    billPaymentCheckLines = billPaymentCheckLines [['TransactionId','TransactionDate','TransactionNo','Account','ItemId','ItemDescription','Quantity','Rate','Total']]

    invoice = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'Invoice.csv', is_csv_file=True )
    invoice.rename(columns = {
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Memo':'PurchaseOrderNo',
        'Txndate':'TransactionDate',
        'Vendorreffullname':'VendNo',
        'Vendoraddressaddr1':'BillName',
        'Vendoraddresscity':'BillCity',
        'Vendoraddressstate':'BillState',
        'Vendoraddresspostalcode':'BillZip',
        'Amountdue':'Total',        
    }, inplace = True)
    invoice['TransactionType'] = 'INVOICE'
    invoiceLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'InvoiceLine.csv', is_csv_file=True )
    invoiceLines = invoiceLines.merge(transactions[['Fqtxnlinkkey', 'Accountreflistid', 'Accountreffullname', 'Amount']], on = ['Fqtxnlinkkey'], how = 'left')
    invoiceLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Amount':'Total'
    }, inplace = True)
    invoiceLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Accountreffullname':'Account',
        'Itemlineitemreffullname':'ItemId',
        'Itemlinedesc':'ItemDescription',
        'Itemlinequantity':'Quantity',
        'Itemlinecost':'Rate',
    }, inplace = True)

    creditCardCharge = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'CreditCardCharge.csv', is_csv_file=True )
    creditCardCharge.rename(columns = {
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Memo':'PurchaseOrderNo',
        'Txndate':'TransactionDate',
        'Vendorreffullname':'VendNo',
        'Vendoraddressaddr1':'BillName',
        'Vendoraddresscity':'BillCity',
        'Vendoraddressstate':'BillState',
        'Vendoraddresspostalcode':'BillZip',
        'Amountdue':'Total',        
    }, inplace = True)
    creditCardCharge['TransactionType'] = 'CREDIT CARD CHARGE'
    creditCardChargeLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'CreditCardChargeExpenseLine.csv', is_csv_file=True )
    creditCardChargeLines.drop(columns =['Accountreflistid', 'Accountreffullname'], inplace = True)
    creditCardChargeLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Expenselineaccountreffullname':'Accountreffullname',
        'Expenselineamount':'Total'
    }, inplace = True)
    creditCardChargeLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Accountreffullname':'Account',
        'Itemlineitemreffullname':'ItemId',
        'Itemlinedesc':'ItemDescription',
        'Itemlinequantity':'Quantity',
        'Itemlinecost':'Rate',
    }, inplace = True)

    creditCardCredit = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'CreditCardCredit.csv', is_csv_file=True )
    creditCardCredit.rename(columns = {
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Memo':'PurchaseOrderNo',
        'Txndate':'TransactionDate',
        'Vendorreffullname':'VendNo',
        'Vendoraddressaddr1':'BillName',
        'Vendoraddresscity':'BillCity',
        'Vendoraddressstate':'BillState',
        'Vendoraddresspostalcode':'BillZip',
        'Amountdue':'Total',        
    }, inplace = True)
    creditCardCredit['TransactionType'] = 'CREDIT CARD CREDIT'
    creditCardCreditLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'CreditCardCreditExpenseLine.csv', is_csv_file=True )
    creditCardCreditLines.drop(columns =['Accountreflistid', 'Accountreffullname'], inplace = True)
    creditCardCreditLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Expenselineaccountreffullname':'Accountreffullname',
        'Expenselineamount':'Total'
    }, inplace = True)
    creditCardCreditLines.rename(columns = {
        'Txndate':'TransactionDate',
        'Txnid':'TransactionId',
        'Refnumber':'TransactionNo',
        'Accountreffullname':'Account',
        'Itemlineitemreffullname':'ItemId',
        'Itemlinedesc':'ItemDescription',
        'Itemlinequantity':'Quantity',
        'Itemlinecost':'Rate',
    }, inplace = True)
    creditCardCreditLines['Total']=-creditCardCreditLines['Total']

    txns = pd.concat([
        generalJournal,
        bills, 
        vendorCredit, 
        checks, 
        salesReceipts, 
        receivePayment, 
        deposit,
        billPaymentCheck,
        invoice,
        creditCardCharge,
        creditCardCredit
    ], ignore_index=True)
    txnsLines = pd.concat([
        generalJournalLines, 
        billsLines, 
        billExpenseLines, 
        vendorCreditLines, 
        vendorCreditExpenseLines, 
        checksLines, 
        checkExpenseLine,
        salesReceiptsLines,
        receivePaymentLines,
        depositLines,
        billPaymentCheckLines,
        invoiceLines,
        creditCardChargeLines,
        creditCardCreditLines
    ], ignore_index=True)
    txns = txns[
        (pd.to_datetime(txns['TransactionDate'], errors='coerce')>=start_date)&\
        (pd.to_datetime(txns['TransactionDate'], errors='coerce')<=end_date)
    ].copy()
    txnsLines = txnsLines[
        (txnsLines['Account'].str.upper().isin(list_of_accounts))&\
        (pd.to_datetime(txnsLines['TransactionDate'], errors='coerce')>=start_date)&\
        (pd.to_datetime(txnsLines['TransactionDate'], errors='coerce')<=end_date)
    ].copy()  
    txnsLines = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txnsLines, df_name = 'txnsLines', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
    txnsLines.ItemId = txnsLines.ItemId.fillna('').astype('str')
    item.ItemId = item.ItemId.fillna('').astype('str')
    txnsLines = txnsLines.merge(item[['ItemId', 'ItemNo', 'ItemName']], on='ItemId', how='left')
    txnsLines = txnsLines[['TransactionId', 'TransactionNo', 'Account', 'ItemId', 'ItemNo', 'ItemName', 'ItemDescription', 'Quantity', 'Rate', 'Total']]
    txnsLines['Company'] = companyName
    txnsLines = txnsLines[['Company'] + txnsLines.columns[:-1].tolist()]
    txns = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns, df_name = 'txns', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
    txns = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns, df_name = 'txns', id_column = ['TransactionId'], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )

    txns['subTotal'] = txns['Total']
    txns['TransactionStatus'] = txns['Ispaid'].fillna('').astype('str').replace({'True': 'PAID IN FULL', 'False': 'NOT PAID IN FULL', '': 'NOT PAID IN FULL'})
    
    txns.VendNo = txns.VendNo.fillna('').astype('str')
    vendors.VendNo = vendors.VendNo.fillna('').astype('str')
    txns = txns.merge(vendors[['VendId', 'VendNo', 'VendName']], on = 'VendNo', how = 'left').copy()
    invoices = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns, df_name = 'txns', id_column = ['TransactionId'], additional_date_columns = [], zip_code_columns = ['BillZip'], state_columns = ['BillState'], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
    
    txns = txns[['PurchaseOrderNo', 'TransactionId', 'TransactionNo', 'TransactionStatus', 'TransactionType', 'TransactionDate', 'VendId', 'VendNo', 'VendName', 'BillName', 'BillCity', 'BillState', 'BillZip', 'subTotal', 'Total']].copy()
    txns['Company'] = companyName
    txns = txns[['Company'] + txns.columns[:-1].tolist()]
    txnsLines = txnsLines[txnsLines['TransactionId'].isin(txns['TransactionId'])]
    return txns, txnsLines


def process_qb_transactions(
    list_of_accounts,
    companyName,
    transactions,
    item,
    customer,
    start_date,
    end_date,
    s3_client,
    s3_bucket_name,
    qodbc=False
):
    if qodbc:
        transactions = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'Transaction.csv', is_csv_file=True )
        invoices = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'Invoice.csv', is_csv_file=True )
        invoicesLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'InvoiceLine.csv', is_csv_file=True )
        invoicesLines=invoicesLines.merge(transactions[['Fqtxnlinkkey', 'Accountreflistid', 'Accountreffullname']], on = 'Fqtxnlinkkey', how = 'left')
        invoicesLines.rename(columns = {
            'Invoicelineamount':'Total'
        }, inplace = True)
        creditMemo = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'CreditMemo.csv', is_csv_file=True )
        creditMemoLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'CreditMemoLine.csv', is_csv_file=True )
        creditMemoLines=creditMemoLines.merge(transactions[['Fqtxnlinkkey', 'Accountreflistid', 'Accountreffullname']], on = 'Fqtxnlinkkey', how = 'left')
        creditMemoLines.rename(columns = {
            'Creditmemolineamount':'Total'
        }, inplace = True)
        creditMemoLines['Total']=-creditMemoLines['Total']
        generalJournal = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'JournalEntry.csv', is_csv_file=True )
        generalJournalLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'JournalEntryLine.csv', is_csv_file=True )
        generalJournalLines.rename(columns = {
            'Journallineaccountreffullname':'Accountreffullname',
            'Journallineamount':'Total'
        }, inplace = True)
        deposits = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'Deposit.csv', is_csv_file=True )
        depositsLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'DepositLine.csv', is_csv_file=True )
        depositsLines.rename(columns = {
            'Depositlineaccountreffullname':'Accountreffullname',
            'Depositlineamount':'Total'
        }, inplace = True)
        payments = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'ReceivePayment.csv', is_csv_file=True )
        paymentsLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'ReceivePaymentLine.csv', is_csv_file=True )
        paymentsLines.rename(columns = {
            'Deposittoaccountreffullname':'Accountreffullname',
            'Totalamount':'Total'
        }, inplace = True)   
        bills = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'Bill.csv', is_csv_file=True )
        billsLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'BillExpenseLine.csv', is_csv_file=True )
        billsLines.rename(columns = {
            'Expenselineaccountreffullname':'Accountreffullname',
            'Expenselineamount':'Total'
        }, inplace = True)
        billsLines['Total']=-billsLines['Total']
        checks = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'Check.csv', is_csv_file=True )
        checksLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'CheckExpenseLine.csv', is_csv_file=True )
        checksLines.drop(columns =['Accountreflistid', 'Accountreffullname'], inplace = True)
        checksLines.rename(columns = {
            'Expenselineaccountreffullname':'Accountreffullname',
            'Expenselineamount':'Total'
        }, inplace = True)
        checksLines['Total']=-checksLines['Total']
        creditCard = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'CreditCardCredit.csv', is_csv_file=True )
        creditCardLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'CreditCardCreditExpenseLine.csv', is_csv_file=True )
        creditCardLines.drop(columns =['Accountreflistid', 'Accountreffullname'], inplace = True)
        creditCardLines.rename(columns = {
            'Expenselineaccountreffullname':'Accountreffullname',
            'Expenselineamount':'Total'
        }, inplace = True)
        creditCardLines['Total']=-creditCardLines['Total']
        invoices['TransactionType'] = 'INVOICE'
        generalJournal['TransactionType'] = 'GENERAL JOURNAL'
        creditMemo['TransactionType'] = 'CREDIT MEMO'
        bills['TransactionType'] = 'BILL'
        deposits['TransactionType'] = 'DEPOSIT'
        payments['TransactionType'] = 'PAYMENT'
        checks['TransactionType'] = 'CHECK'
        creditCard['TransactionType'] = 'CREDIT CARD'
        txns = pd.concat([invoices, generalJournal, creditMemo, bills, deposits, payments, checks, creditCard], ignore_index=True)
        txnsLines = pd.concat([invoicesLines, generalJournalLines, creditMemoLines, billsLines, depositsLines, paymentsLines, checksLines, creditCardLines], ignore_index=True)
        txns = txns[
            (pd.to_datetime(txns['Txndate'], errors='coerce')>=start_date)&\
            (pd.to_datetime(txns['Txndate'], errors='coerce')<=end_date)
        ].copy()
        txnsLines = txnsLines[
            (txnsLines['Accountreffullname'].str.upper().isin(list_of_accounts))&\
            (pd.to_datetime(txnsLines['Txndate'], errors='coerce')>=start_date)&\
            (pd.to_datetime(txnsLines['Txndate'], errors='coerce')<=end_date)
        ].copy()  
        txnsLines.rename(columns = {
            'Txnid':'TransactionId',
            'Refnumber':'TransactionNo',
            'Accountreffullname':'Account',
            'Invoicelineitemreffullname':'ItemId',
            'Invoicelinedesc':'ItemDescription',
            'Invoicelinequantity':'Quantity',
            'Invoicelinerate':'Rate',
        }, inplace = True)
        txnsLines = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txnsLines, df_name = 'txnsLines', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        txnsLines.ItemId = txnsLines.ItemId.fillna('').astype('str')
        item.ItemId = item.ItemId.fillna('').astype('str')
        txnsLines = txnsLines.merge(item[['ItemId', 'ItemNo', 'ItemName']], on='ItemId', how='left')
        txnsLines = txnsLines[['TransactionId', 'TransactionNo', 'Account', 'ItemId', 'ItemNo', 'ItemName', 'ItemDescription', 'Quantity', 'Rate', 'Total']]
        txnsLines['Company'] = companyName
        txnsLines = txnsLines[['Company'] + txnsLines.columns[:-1].tolist()]
        txns = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns, df_name = 'txns', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        txns = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns, df_name = 'txns', id_column = ['Txnid'], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        txns.rename(columns = {
            'Txnid':'TransactionId',
            'Refnumber':'TransactionNo',
            'Fob':'OrderNo',
            'TRNSTYPE':'TransactionType',
            'Txndate':'TransactionDate',
            'PAID':'TransactionStatus',
            'Salesrepreffullname':'SalesRepID',
            'Customerreffullname':'CustNo',
            'Ponumber':'CustPo',
            'SHIPDATE':'ShipDate',
            'Billaddressaddr1':'BillName',
            'Billaddresscity':'BillCity',
            'Billaddressstate':'BillState',
            'Billaddresspostalcode':'BillZip',
            'Shipaddressaddr1':'ShipName',
            'Shipaddresscity':'ShipCity',
            'Shipaddressstate':'ShipState',
            'Shipaddresspostalcode':'ShipZip',
            'Subtotal':'Total',
        }, inplace = True)
        txns['subTotal'] = txns['Total']
        txns['TransactionStatus'] = txns['Ispaid'].fillna('').astype('str').replace({'True': 'INVOICED IN FULL', 'False': 'NOT INVOICED IN FULL', '': 'NOT INVOICED IN FULL'})

        txns.CustNo = txns.CustNo.fillna('').astype('str')
        customer.CustNo = customer.CustNo.fillna('').astype('str')
        txns = txns.merge(customer[['CustId', 'CustNo', 'CustName']], on = 'CustNo', how = 'left').copy()
        invoices = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns, df_name = 'txns', id_column = ['TransactionId'], additional_date_columns = [], zip_code_columns = ['BillZip'], state_columns = ['BillState'], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )

        txns = txns[['OrderNo', 'TransactionId', 'TransactionNo', 'TransactionStatus', 'TransactionType', 'TransactionDate', 'SalesRepID', 'CustPo', 'CustId', 'CustNo', 'CustName', 'ShipName', 'ShipCity', 'ShipState', 'ShipZip', 'BillName', 'BillCity', 'BillState', 'BillZip', 'subTotal', 'Total']].copy()
        txns['Company'] = companyName
        txns = txns[['Company'] + txns.columns[:-1].tolist()]
        txnsLines = txnsLines[txnsLines['TransactionId'].isin(txns['TransactionId'])]

    else:
        for txnsType in [
            ('GENERAL JOURNAL'),
            ('CREDIT MEMO'),
            ('INVOICE'),
            ('BILL'),
            ('DEPOSIT'),
            ('PAYMENT'),
            ('CHECK'),
            ('CREDIT CARD'),
        ]:
            txns, txnsLines = extract_transaction_header_line(transactions, txnsType)
            txns = txns[
                (pd.to_datetime(txns['DATE'], errors='coerce')>=start_date)&\
                (pd.to_datetime(txns['DATE'], errors='coerce')<=end_date)
            ].copy()
            txnsLines = txnsLines[
                (txnsLines['ACCNT'].str.upper().isin(list_of_accounts))&\
                (pd.to_datetime(txnsLines['DATE'], errors='coerce')>=start_date)&\
                (pd.to_datetime(txnsLines['DATE'], errors='coerce')<=end_date)
            ].copy()  
            txnsLines.rename(columns = {
                'SPLID':'TransactionId',
                'DOCNUM':'TransactionNo',
                'ACCNT':'Account',
                'INVITEM':'ItemId',
                'MEMO':'ItemDescription',
                'QNTY':'Quantity',
                'PRICE':'Rate',
                'AMOUNT':'Total'
            }, inplace = True)
            txnsLines['ItemDescription'] = txnsLines['ItemDescription'].fillna('').astype('str').str.replace(r'\\n', ' ', regex=True)
            txnsLines['TransactionId'] = txnsLines['TransactionId'].fillna('').astype('str').apply(convert_to_int_or_keep)
            txnsLines = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txnsLines, df_name = 'txnsLines', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
            txnsLines['Quantity'] = txnsLines['Quantity'].fillna(0).astype('str').str.replace(',', '').astype('float') * -1
            txnsLines['Rate'] = txnsLines['Rate'].fillna(0).astype('str').str.replace(',', '').apply(lambda x: float(x.replace('%', '')) / 100 if '%' in x else float(x))
            txnsLines['Total'] = txnsLines['Total'].fillna(0).astype('str').str.replace(',', '').astype('float') * -1
            txnsLines.loc[txnsLines['TransactionNo'].isna(), 'Total'] = txnsLines['Total'] * -1
            txnsLines = txnsLines[['TransactionId', 'TransactionNo', 'Account', 'ItemId', 'ItemDescription', 'Quantity', 'Rate', 'Total']]
            txnsLines[['Quantity', 'Rate', 'Total']] = txnsLines[['Quantity', 'Rate', 'Total']].fillna(0)
            txnsLines.ItemId = txnsLines.ItemId.fillna('').astype('str')
            item.ItemId = item.ItemId.fillna('').astype('str')
            txnsLines = txnsLines.merge(item[['ItemId', 'ItemNo', 'ItemName']], on='ItemId', how='left')
            txnsLines = txnsLines[['TransactionId', 'TransactionNo', 'Account', 'ItemId', 'ItemNo', 'ItemName', 'ItemDescription', 'Quantity', 'Rate', 'Total']]
            txnsLines['Company'] = companyName
            txnsLines = txnsLines[['Company'] + txnsLines.columns[:-1].tolist()]
            txns = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns, df_name = 'txns', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
            txns = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns, df_name = 'txns', id_column = ['TRNSID'], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
            txns.rename(columns = {
                'TRNSID':'TransactionId',
                'DOCNUM':'TransactionNo',   
                'TRNSTYPE':'TransactionType',
                'DATE':'TransactionDate',
                'PAID':'TransactionStatus',
                'REP':'SalesRepID',
                'NAME':'CustNo',
                'PONUM':'CustPo',
                'SHIPDATE':'ShipDate',
                'AddressName':'BillName',
                'AddressCity':'BillCity',
                'AddressState':'BillState',
                'AddressZip':'BillZip',
                'ShippingAddressName':'ShipName',
                'ShippingAddressCity':'ShipCity',
                'ShippingAddressState':'ShipState',
                'ShippingAddressZip':'ShipZip',
            }, inplace = True)
            extra_txns = txns[ (txns['ACCNT'].str.upper().isin(list_of_accounts)) ].copy()
            extra_txns.rename(columns = { 'AMOUNT':'Total' }, inplace = True)
            extra_txns['Total'] = extra_txns['Total'].fillna(0).astype('str').str.replace(',', '').astype('float') * -1
            extra_txnsLines = extra_txns.copy()
            extra_txnsLines.rename(columns = {
                'TRNSID':'TransactionId',
                'ACCNT':'Account',
                'DOCNUM':'TransactionNo',
                'MEMO':'ItemDescription',
                'AMOUNT':'Total'
            }, inplace = True)
            extra_txnsLines['ItemDescription'] = extra_txnsLines['ItemDescription'].fillna('').astype('str').str.replace(r'\\n', ' ', regex=True)
            extra_txnsLines[['ItemId', 'ItemNo', 'ItemName', 'CommonName']] = np.nan
            extra_txnsLines['Quantity'] = 0.0
            extra_txnsLines['Rate'] = 0.0
            extra_txnsLines = extra_txnsLines[['TransactionId', 'TransactionNo', 'Account', 'ItemId', 'ItemNo', 'ItemName', 'CommonName', 'ItemDescription', 'Quantity', 'Rate', 'Total']]
            extra_txnsLines['Company'] = companyName
            extra_txnsLines = extra_txnsLines[['Company'] + extra_txnsLines.columns[:-1].tolist()]
            extra_txns.drop(columns = ['Total'], inplace = True)
            if txnsType == 'GENERAL JOURNAL':    
                txns = pd.concat([txns, extra_txns[~extra_txns['TransactionId'].isin(txns['TransactionId'])]], ignore_index=True)
                txnsLines = pd.concat([txnsLines, extra_txnsLines], ignore_index=True)
            txns.TransactionId = txns.TransactionId.fillna('').astype('str')
            txnsLines.TransactionId = txnsLines.TransactionId.fillna('').astype('str')
            txns = txns.merge(
                txnsLines.groupby('TransactionId').agg(Total = ('Total', 'sum')).reset_index(),
                on='TransactionId',
            )     
            txns['subTotal'] = txns['Total']
            txns['TransactionId'] = txns['TransactionId'].fillna('').astype('str')
            txns['TransactionId'] = txns['TransactionId'].apply(convert_to_int_or_keep)
            txns['TransactionStatus'] = txns['TransactionStatus'].fillna('').astype('str').replace({'Y': 'INVOICED IN FULL', 'N': 'NOT INVOICED IN FULL'})
            txns.SalesRepID = txns.SalesRepID.fillna('').astype('str').str.split(':').str[-1]
            txns = txns[[i for i in txns.columns if i in ['TransactionId', 'TransactionNo', 'TransactionType', 'TransactionDate', 'TransactionStatus', 'ShipDate', 'SalesRepID', 'CustPo', 'CustNo', 'BillName', 'BillCity', 'BillState', 'BillZip', 'ShipName', 'ShipCity', 'ShipState', 'ShipZip', 'subTotal', 'Total']]].copy()
            if txnsType == 'GENERAL JOURNAL':
                generalJournalLines = txnsLines.copy()
                generalJournalLines['ItemId'] = 'GENERAL JOURNAL'
                generalJournalLines['ItemNo'] = 'GENERAL JOURNAL'
                generalJournalLines['ItemName'] = 'GENERAL JOURNAL'
                generalJournal = txns.copy()
            elif txnsType == 'CREDIT MEMO':
                creditMemoLines = txnsLines.copy()
                creditMemo = txns.copy()
            elif txnsType == 'INVOICE':
                invoicesLines = txnsLines.copy()
                invoices = txns.copy()
            elif txnsType == 'BILL':
                billsLines = txnsLines.copy()
                billsLines['ItemId'] = 'BILL'
                billsLines['ItemNo'] = 'BILL'
                billsLines['ItemName'] = 'BILL'
                bills = txns.copy()
            elif txnsType == 'DEPOSIT':
                depositsLines = txnsLines.copy()
                depositsLines['ItemId'] = 'DEPOSIT'
                depositsLines['ItemNo'] = 'DEPOSIT'
                depositsLines['ItemName'] = 'DEPOSIT'
                deposits = txns.copy()
            elif txnsType == 'PAYMENT':
                paymentsLines = txnsLines.copy()
                payments = txns.copy()
            elif txnsType == 'CHECK':
                checksLines = txnsLines.copy()
                checks = txns.copy()
            elif txnsType == 'CREDIT CARD':
                creditCardLines = txnsLines.copy()
                creditCard = txns.copy()    
        # #-----------------------------------------------------------------------------------------------------------
        SalesOrderLinkedTxn = read_csv_from_s3(s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'SalesOrderLinkedTxn.xlsx', encoding = 'Windows-1252', is_csv_file=False)
        SalesOrderLinkedTxn = SalesOrderLinkedTxn[SalesOrderLinkedTxn['LinkedTxnTxnType']=='Invoice'].copy()
        SalesOrderLinkedTxn.rename(columns = {
            'RefNumber':'OrderNo',
            'LinkedTxnRefNumber':'TransactionNo',                   
        }, inplace = True)
        SalesOrderLinkedTxn = SalesOrderLinkedTxn[['OrderNo','TransactionNo']].copy()
        SalesOrderLinkedTxn.TransactionNo = SalesOrderLinkedTxn.TransactionNo.fillna('').astype('str')
        invoices.TransactionNo = invoices.TransactionNo.fillna('').astype('str')
        invoices = invoices.merge(SalesOrderLinkedTxn.drop_duplicates(subset=['TransactionNo']), on='TransactionNo', how = 'left')
        # #-----------------------------------------------------------------------------------------------------------
        invoices.CustNo = invoices.CustNo.fillna('').astype('str')
        customer.CustNo = customer.CustNo.fillna('').astype('str')
        invoices = invoices.merge(customer[['CustId', 'CustNo', 'CustName']], on = 'CustNo', how = 'left').copy()
        invoices = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = invoices, df_name = 'invoices', id_column = ['TransactionId'], additional_date_columns = [], zip_code_columns = ['BillZip'], state_columns = ['BillState'], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        #-----------------------------------------------------------------------------------------------------------
        txns = pd.concat([invoices, generalJournal, creditMemo, bills, deposits, payments, checks, creditCard], ignore_index=True)  
        txnsLines = pd.concat([invoicesLines, generalJournalLines, creditMemoLines, billsLines, depositsLines, paymentsLines, checksLines, creditCardLines], ignore_index=True)
        #-----------------------------------------------------------------------------------------------------------
        txns.TransactionId = txns.TransactionId.fillna('').astype('str')
        txnsLines.TransactionId = txnsLines.TransactionId.fillna('').astype('str')
        mismatched_txns = txns.merge(txnsLines, on='TransactionId', how='inner', suffixes=('_ord', '_lin')).groupby('TransactionId').agg({'subTotal':'max', 'Total_lin':'sum'}).reset_index()
        mismatched_txns = mismatched_txns[~np.isclose(mismatched_txns['subTotal'], mismatched_txns['Total_lin'], atol=0.1)]
        print(f"{mismatched_txns.shape[0]} txns Total do not match orderline Total")
        txns = txns[~txns['TransactionId'].isin(mismatched_txns['TransactionId'])]
        txns['TransactionId'] = txns['TransactionId']
        txns['TransactionId'] = txns['TransactionId'].str.split(' :: ').str[0]
        txns = txns[['OrderNo', 'TransactionId', 'TransactionNo', 'TransactionStatus', 'TransactionType', 'TransactionDate', 'SalesRepID', 'CustPo', 'CustId', 'CustNo', 'CustName', 'ShipName', 'ShipCity', 'ShipState', 'ShipZip', 'BillName', 'BillCity', 'BillState', 'BillZip', 'subTotal', 'Total']].copy()
        txns = txns[~txns['TransactionId'].fillna('').astype('str').str.upper().duplicated()]
        txns['Company'] = companyName
        txns = txns[['Company'] + txns.columns[:-1].tolist()]
        txnsLines = txnsLines[txnsLines['TransactionId'].isin(txns['TransactionId'])]
    return txns, txnsLines


def process_qb_orders(
    companyName,
    transactions,
    item,
    item_df,
    customersORvendors,
    start_date,
    end_date,
    s3_client,
    s3_bucket_name,
    txnsType,
    txnsType2,
    txnsType3,
    txnsType4,
    txnsType5,
    orderCloseDates,
    DBIA,
    itemsCategoriesV3,
    SalesOrderLinkedTxn,
    qodbc=False
):
    if qodbc:
        prompt = f'{txnsType2}...'
        print(prompt)
        write_file('log.txt' , f"{print_date_time()}\t\t{prompt}")
        #### orders
        if txnsType == 'PURCHORD':
            object_key_1 = 'PurchaseOrder.csv'
            object_key_2 = 'PurchaseOrderLine.csv'
            object_key_3 = 'Isfullyreceived'
            object_key_4 = 'Inventorysitereffullname'
            object_key_5 = 'Vendorreffullname'
            object_key_6 = 'Txnnumber'
            object_key_7 = 'Expecteddate'
            object_key_8 = 'Totalamount'
            object_key_9 = 'Purchase'
        else:
            object_key_1 = 'SalesOrder.csv'
            object_key_2 = 'SalesOrderLine.csv'
            object_key_3 = 'Isfullyinvoiced'
            object_key_4 = 'Salesrepreffullname'
            object_key_5 = 'Customerreffullname'
            object_key_6 = 'Ponumber'
            object_key_7 = 'Shipdate'
            object_key_8 = 'Subtotal'
            object_key_9 = 'Sales'
        orders = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = object_key_1, is_csv_file=True )
        ordersLines = read_csv_from_s3( s3_client = s3_client, bucket_name = s3_bucket_name, object_key = object_key_2, is_csv_file=True )
        orders = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = orders, df_name = 'orders', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        orders.rename(columns = {
            'Txnid':f'{txnsType2}Id',
            'Refnumber':f'{txnsType2}No',
            'Txndate':f'{txnsType2}Date',
            object_key_4:txnsType4,
            object_key_5:f'{txnsType3}No',
            object_key_6:txnsType5,
            object_key_7:'ShipDate',
            'Shipaddressaddr1':'ShipName',
            'Shipaddresscity':'ShipCity',
            'Shipaddressstate':'ShipState',
            'Shipaddresspostalcode':'ShipZip',
            object_key_8:'Total'
        }, inplace = True)
        orders[f'{txnsType2}Id'] = orders[f'{txnsType2}Id'].fillna('').astype('str')
        orders[f'{txnsType2}Status'] = 'Open'
        orders.loc[(orders['Ismanuallyclosed']==1)|(orders[object_key_3]==1), f'{txnsType2}Status'] = 'Closed'
        orders = orders[[f'{txnsType2}Id', f'{txnsType2}No', f'{txnsType2}Date', f'{txnsType2}Status', 'ShipDate', txnsType4, txnsType5, f'{txnsType3}No', 'ShipName', 'ShipCity', 'ShipState', 'ShipZip', 'Total']].copy()
        orders = orders.copy()
        orders = orders[
            (pd.to_datetime(orders[f'{txnsType2}Date'], errors='coerce')>=start_date)&\
            (pd.to_datetime(orders[f'{txnsType2}Date'], errors='coerce')<=end_date)
        ].copy()
        orders = orders.copy()
        #### Lines
        ordersLines.rename(columns = {
            'Txnid':f'{txnsType2}Id',
            'Refnumber':f'{txnsType2}No',
            f'{object_key_9}orderlineitemreffullname':'ItemId',
            f'{object_key_9}orderlinedesc':'ItemDescription',
            f'{object_key_9}orderlinequantity':'Quantity',
            f'{object_key_9}orderlinerate':'Rate',
            f'{object_key_9}orderlineamount':'Total'
        }, inplace = True)
        ordersLines['ItemDescription'] = ordersLines['ItemDescription'].fillna('').astype('str').str.replace(r'\\n', ' ', regex=True)
        ordersLines = ordersLines[~ordersLines[f'{txnsType2}Id'].isna()].copy()
        ordersLines[f'{txnsType2}Id'] = ordersLines[f'{txnsType2}Id'].fillna('').astype('str')
        ordersLines = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = ordersLines, df_name = 'ordersLines', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        ordersLines['Quantity'] = ordersLines['Quantity'].fillna(0).astype('str').str.replace(',', '').astype('float')
        ordersLines['Total'] = ordersLines['Total'].fillna(0).astype('str').str.replace(',', '').astype('float')
        ordersLines['Rate'] = ordersLines['Rate'].fillna(0).astype('str').str.replace(',', '').apply(lambda x: float(x.replace('%', '')) / 100 if '%' in x else float(x))
        ordersLines = ordersLines[[f'{txnsType2}Id', f'{txnsType2}No', 'ItemId', 'ItemDescription', 'Quantity', 'Rate', 'Total']]
        ordersLines = ordersLines.copy()
        ordersLines[['Quantity', 'Rate', 'Total']] = ordersLines[['Quantity', 'Rate', 'Total']].fillna(0)
        ordersLines.ItemId = ordersLines.ItemId.fillna('').astype('str')
        item.ItemId = item.ItemId.fillna('').astype('str')
        ordersLines = ordersLines.merge(item[['ItemId', 'ItemNo', 'ItemName']], on='ItemId', how='left')
        ordersLines.loc[ordersLines['ItemId']=='0', 'ItemId']='ITEM'
        ordersLines.loc[ordersLines['ItemId']=='0', 'ItemNo']='ITEM'
        ordersLines.loc[ordersLines['ItemId']=='0', 'ItemName']='ITEM'
        ordersLines = ordersLines[[f'{txnsType2}Id', f'{txnsType2}No', 'ItemId', 'ItemNo', 'ItemName', 'ItemDescription', 'Quantity', 'Rate', 'Total']]
        ordersLines['Company'] = companyName
        ordersLines = ordersLines[['Company'] + ordersLines.columns[:-1].tolist()]
        ordersLines = ordersLines.copy()
        orders[f'{txnsType2}Id'] = orders[f'{txnsType2}Id'].fillna('').astype('str')
        ordersLines[f'{txnsType2}Id'] = ordersLines[f'{txnsType2}Id'].fillna('').astype('str')
        ordersLines = ordersLines.merge(orders[[f'{txnsType2}Id','ShipDate']], on = f'{txnsType2}Id', how = 'left')
        orders.drop(columns=['ShipDate'], inplace=True)
        orders[f'{txnsType2}No'] = orders[f'{txnsType2}No'].fillna('').astype('str')
        orderCloseDates[f'{txnsType2}No'] = orderCloseDates[f'{txnsType2}No'].fillna('').astype('str')
        orders = orders.merge(orderCloseDates, on = f'{txnsType2}No', how = 'left')
        if not orders.empty:
            orders.loc[orders[f'{txnsType2}Status']=='Open', 'CloseDate'] = pd.NaT
        else:
            orders['CloseDate'] = pd.NaT
        orders[f'{txnsType3}No'] = orders[f'{txnsType3}No'].fillna('').astype('str')
        customersORvendors[f'{txnsType3}No'] = customersORvendors[f'{txnsType3}No'].fillna('').astype('str')
        orders = orders.merge(customersORvendors[[f'{txnsType3}Id', f'{txnsType3}No', f'{txnsType3}Name']], on = f'{txnsType3}No', how = 'left')
        orders = orders[[f'{txnsType2}Id', f'{txnsType2}No', f'{txnsType2}Status', f'{txnsType2}Date', 'CloseDate', txnsType4, txnsType5, f'{txnsType3}Id', f'{txnsType3}No', f'{txnsType3}Name', 'ShipName', 'ShipCity', 'ShipState', 'ShipZip', 'Total']].copy()
        orders = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = orders, df_name = 'orders', id_column = [f'{txnsType2}Id'], additional_date_columns = [], zip_code_columns = ['ShipZip'], state_columns = ['ShipState'], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        orders = orders[~orders[f'{txnsType2}Id'].str.upper().duplicated()]
        orders[f'{txnsType2}Id'] = orders[f'{txnsType2}Id'].fillna('').astype('str')
        ordersLines[f'{txnsType2}Id'] = ordersLines[f'{txnsType2}Id'].fillna('').astype('str')
        mismatched_orders = orders.merge(ordersLines, on=f'{txnsType2}Id', how='inner', suffixes=('_ord', '_lin')).groupby(f'{txnsType2}Id').agg({'Total_ord':'max', 'Total_lin':'sum'}).reset_index()
        mismatched_orders = mismatched_orders[~np.isclose(mismatched_orders['Total_ord'], mismatched_orders['Total_lin'], atol=0.1)]
        print(f"{mismatched_orders.shape[0]} orders total do not match orderline total")
        orders = orders[~orders[f'{txnsType2}Id'].isin(mismatched_orders[f'{txnsType2}Id'])]
        #-----------------------------------------------------------------------------------------------------------
        orders[f'{txnsType3}Id'] = orders[f'{txnsType3}No'].copy()
        #-----------------------------------------------------------------------------------------------------------
        #-------------------------------------
        orders = orders.drop_duplicates(subset=[f'{txnsType2}Id'])
        orders = orders.loc[orders[f'{txnsType2}Id'].notna() & (orders[f'{txnsType2}Id'].fillna('').astype('str').str.strip() != '')]
        orders['Company'] = companyName
        orders = orders[['Company'] + orders.columns[:-1].tolist()]

        #-------------------------------------
        ordersLines = ordersLines[ordersLines[f'{txnsType2}Id'].isin(orders[f'{txnsType2}Id'])]
        #-------------------------------------
        ordersLines, itemsCategoriesV3, item_df = enrich_and_classify_items(
            item_df, 
            companyName, 
            s3_client, 
            s3_bucket_name, 
            DBIA, 
            itemsCategoriesV3,
            ordersLines
        )
        #-----------------------------------------------------------------------------------------------------------
        orderTypes = ordersLines.merge(
                            itemsCategoriesV3[['index', 'ItemLevel2']] \
                            .rename(columns = {'index':'ItemId'}) \
                            .drop_duplicates(subset = 'ItemId'), on = 'ItemId'
                        ) \
                        .rename(columns={'ItemLevel2': f'{txnsType2}Type'}) \
                        .sort_values([f'{txnsType2}Id', 'Total'], ascending=[True, False]) \
                        .groupby(f'{txnsType2}Id').agg({f'{txnsType2}Type': 'first'}).reset_index()

        orders[f'{txnsType2}Id'] = orders[f'{txnsType2}Id'].fillna('').astype('str')
        orderTypes[f'{txnsType2}Id'] = orderTypes[f'{txnsType2}Id'].fillna('').astype('str')
        orders = orders.merge(orderTypes, on = f'{txnsType2}Id', how = 'left')
        #-----------------------------------------------------------------------------------------------------------
        if companyName == 'KOMAR':  
            ordersLines = ordersLines.merge(orders[[f'{txnsType2}Id', f'{txnsType3}No']], on = f'{txnsType2}Id', how = 'left').rename(columns = {f'{txnsType3}No' : 'SerialNo'})
            ordersLines['SerialNo'] = ordersLines['SerialNo'].apply(
                lambda x: re.search(r'(?<!\d)(\d{4})(?!\d)', str(x)).group() if re.search(r'(?<!\d)(\d{4})(?!\d)', str(x)) else None
            )
            shipInstallDates = read_csv_from_s3(s3_client = s3_client, bucket_name = 'manual-db', object_key = 'serialNumberEngineeringReferenceMaster.csv')[['Serial Number', 'Installation Date']].rename(columns = {'Serial Number' : 'SerialNo', 'Ship Date' : 'ShipDate', 'Installation Date' : 'InstallDate'})
            shipInstallDates = shipInstallDates[~shipInstallDates.SerialNo.duplicated()]
            shipInstallDates.SerialNo = shipInstallDates.SerialNo.fillna('').astype('str')
            ordersLines.SerialNo = ordersLines.SerialNo.fillna('').astype('str')
            shipInstallDates['InstallDate'] = pd.to_datetime(shipInstallDates['InstallDate'], errors='coerce')
            ordersLines = ordersLines.merge(shipInstallDates, on = 'SerialNo', how = 'left')
        else:
            ordersLines['InstallDate'] = np.nan
        #-----------------------------------------------------------------------------------------------------------
        ordersLines.loc[
            (pd.to_datetime(ordersLines['ShipDate']) < start_date) |
            (pd.to_datetime(ordersLines['ShipDate']) > end_date),
            'ShipDate'
        ] = np.nan
        ordersLines.loc[
            (pd.to_datetime(ordersLines['InstallDate']) < start_date) |
            (pd.to_datetime(ordersLines['InstallDate']) > end_date),
            'InstallDate'
        ] = np.nan
        #-----------------------------------------------------------------------------------------------------------
        SalesOrderLinkedTxn[f'{txnsType2}No'] = SalesOrderLinkedTxn[f'{txnsType2}No'].fillna('').astype('str')
        ordersLines[f'{txnsType2}No'] = ordersLines[f'{txnsType2}No'].fillna('').astype('str')
        ordersLines = ordersLines.merge(SalesOrderLinkedTxn, on = f'{txnsType2}No', how = 'left')
        #-----------------------------------------------------------------------------------------------------------
        orders[f'{txnsType2}Id'] = orders[f'{txnsType2}Id'].fillna('').astype('str')
        ordersLines[f'{txnsType2}Id'] = ordersLines[f'{txnsType2}Id'].fillna('').astype('str')
        ordersLines = ordersLines.merge(orders[[f'{txnsType2}Id', f'{txnsType2}Status']], on = f'{txnsType2}Id', how = 'left').rename(columns={f'{txnsType2}Status':'ItemStatus'})
        ordersLines.loc[(ordersLines['ShipDate'].notna()), 'ItemStatus'] = 'SHIPPED'
        ordersLines.loc[(ordersLines['InstallDate'].notna()), 'ItemStatus'] = 'INSTALLED'
        ordersLines.loc[(ordersLines['InvoiceDate'].notna()), 'ItemStatus'] = 'INVOICED'
        #-----------------------------------------------------------------------------------------------------------
        ordersLines.rename(columns = {'CommonName':'ItemType'}, inplace = True)
        #-----------------------------------------------------------------------------------------------------------
    else:
        prompt = f'{txnsType2}...'
        print(prompt)
        write_file('log.txt' , f"{print_date_time()}\t\t{prompt}")
        #### orders
        txns_df, txnsLines = extract_transaction_header_line(transactions, txnsType)
        txns_df = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns_df, df_name = 'txns_df', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        txns_df = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns_df, df_name = 'txns_df', id_column = ['TRNSID'], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        txns_df = txns_df.merge(
            txnsLines[txnsLines['EXTRA'] == 'AUTOSTAX'][['DOCNUM', 'AMOUNT']].rename(columns = {'AMOUNT':'TAX'}),
            on='DOCNUM',
            how='left',
            suffixes=('_iif', '_lines')
        )
        if txnsType == 'PURCHORD':
            txns_df['AMOUNT'] = txns_df['AMOUNT'].fillna(0).astype('str').str.replace(',', '').astype('float')*-1
        else:
            txns_df['AMOUNT'] = txns_df['AMOUNT'].fillna(0).astype('str').str.replace(',', '').astype('float')
        txns_df['TAX'] = txns_df['TAX'].fillna(0).astype('str').str.replace(',', '').astype('float')
        txns_df['subTotal'] = txns_df['AMOUNT'] + txns_df['TAX']
        txns_df.rename(columns = {
            'TRNSID':f'{txnsType2}Id',
            'DOCNUM':f'{txnsType2}No',
            'DATE':f'{txnsType2}Date',
            'PAID':f'{txnsType2}Status',
            'REP':txnsType4,
            'NAME':f'{txnsType3}No',
            'PONUM':txnsType5,
            'SHIPDATE':'ShipDate',
            'AddressName':'BillName',
            'AddressCity':'BillCity',
            'AddressState':'BillState',
            'AddressZip':'BillZip',
            'ShippingAddressName':'ShipName',
            'ShippingAddressCity':'ShipCity',
            'ShippingAddressState':'ShipState',
            'ShippingAddressZip':'ShipZip',
            'AMOUNT':'Total',
        }, inplace = True)
        txns_df[f'{txnsType2}Id'] = txns_df[f'{txnsType2}Id'].fillna('').astype('str')
        txns_df[f'{txnsType2}Id'] = txns_df[f'{txnsType2}Id'].apply(convert_to_int_or_keep)
        txns_df[f'{txnsType2}Status'] = txns_df[f'{txnsType2}Status'].fillna('').astype('str').replace({'Y': 'INVOICED IN FULL', 'N': 'NOT INVOICED IN FULL'})
        txns_df[txnsType4] = txns_df[txnsType4].fillna('').astype('str').str.split(':').str[-1]
        txns_df = txns_df[[f'{txnsType2}Id', f'{txnsType2}No', f'{txnsType2}Date', f'{txnsType2}Status', 'ShipDate', txnsType4, txnsType5, f'{txnsType3}No', 'BillName', 'BillCity', 'BillState', 'BillZip', 'ShipName', 'ShipCity', 'ShipState', 'ShipZip', 'subTotal', 'Total']].copy()
        txns_df = txns_df.copy()
        txns_df = txns_df[pd.to_datetime(txns_df[f'{txnsType2}Date'], errors='coerce') > start_date]
        txns_df = txns_df[pd.to_datetime(txns_df[f'{txnsType2}Date'], errors='coerce') < end_date]
        orders = txns_df.copy()
        #### Lines
        _, txnsLines = extract_transaction_header_line(transactions, txnsType)
        txnsLines = txnsLines[txnsLines['EXTRA'] != 'AUTOSTAX']
        txnsLines.rename(columns = {
            'SPLID':f'{txnsType2}Id',
            'DOCNUM':f'{txnsType2}No',
            'INVITEM':'ItemId',
            'MEMO':'ItemDescription',
            'QNTY':'Quantity',
            'PRICE':'Rate',
            'AMOUNT':'Total'
        }, inplace = True)
        txnsLines['ItemDescription'] = txnsLines['ItemDescription'].fillna('').astype('str').str.replace(r'\\n', ' ', regex=True)
        txnsLines = txnsLines[~txnsLines[f'{txnsType2}Id'].isna()].copy()
        txnsLines[f'{txnsType2}Id'] = txnsLines[f'{txnsType2}Id'].fillna('').astype('str')
        txnsLines[f'{txnsType2}Id'] = txnsLines[f'{txnsType2}Id'].apply(convert_to_int_or_keep)
        txnsLines = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txnsLines, df_name = 'txnsLines', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        if txnsType == 'PURCHORD':
            txnsLines['Quantity'] = txnsLines['Quantity'].fillna(0).astype('str').str.replace(',', '').astype('float')
            txnsLines['Total'] = txnsLines['Total'].fillna(0).astype('str').str.replace(',', '').astype('float')
        else:
            txnsLines['Quantity'] = txnsLines['Quantity'].fillna(0).astype('str').str.replace(',', '').astype('float') * -1
            txnsLines['Total'] = txnsLines['Total'].fillna(0).astype('str').str.replace(',', '').astype('float') * -1
        txnsLines['Rate'] = txnsLines['Rate'].fillna(0).astype('str').str.replace(',', '').apply(lambda x: float(x.replace('%', '')) / 100 if '%' in x else float(x))
        txnsLines = txnsLines[[f'{txnsType2}Id', f'{txnsType2}No', 'ItemId', 'ItemDescription', 'Quantity', 'Rate', 'Total']]
        txnsLines = txnsLines.copy()
        txnsLines[['Quantity', 'Rate', 'Total']] = txnsLines[['Quantity', 'Rate', 'Total']].fillna(0)
        txnsLines.ItemId = txnsLines.ItemId.fillna('').astype('str')
        item.ItemId = item.ItemId.fillna('').astype('str')
        txnsLines = txnsLines.merge(item[['ItemId', 'ItemNo', 'ItemName']], on='ItemId', how='left')
        txnsLines.loc[txnsLines['ItemId']=='0', 'ItemId']='ITEM'
        txnsLines.loc[txnsLines['ItemId']=='0', 'ItemNo']='ITEM'
        txnsLines.loc[txnsLines['ItemId']=='0', 'ItemName']='ITEM'
        txnsLines = txnsLines[[f'{txnsType2}Id', f'{txnsType2}No', 'ItemId', 'ItemNo', 'ItemName', 'ItemDescription', 'Quantity', 'Rate', 'Total']]
        txnsLines['Company'] = companyName
        txnsLines = txnsLines[['Company'] + txnsLines.columns[:-1].tolist()]
        ordersLines = txnsLines.copy()
        orders[f'{txnsType2}Id'] = orders[f'{txnsType2}Id'].fillna('').astype('str')
        ordersLines[f'{txnsType2}Id'] = ordersLines[f'{txnsType2}Id'].fillna('').astype('str')
        ordersLines = ordersLines.merge(orders[[f'{txnsType2}Id','ShipDate']], on = f'{txnsType2}Id', how = 'left')
        orders.drop(columns=['ShipDate'], inplace=True)

        orders.loc[orders[f'{txnsType2}Status'].fillna('').astype('str') == 'NOT INVOICED IN FULL', f'{txnsType2}Status'] = 'Open'
        orders.loc[orders[f'{txnsType2}Status'].fillna('').astype('str') == 'INVOICED IN FULL', f'{txnsType2}Status'] = 'Closed'
        SalesOrder = read_csv_from_s3(s3_client = s3_client, bucket_name = s3_bucket_name, object_key = 'SalesOrder.xlsx', encoding = 'Windows-1252', is_csv_file=False)
        closedOrders = SalesOrder.loc[(SalesOrder['IsManuallyClosed']==1)|(SalesOrder['IsFullyInvoiced']==1)]
        closedOrders = closedOrders['RefNumber'].fillna('').astype('str')
        orders.loc[orders[f'{txnsType2}No'].fillna('').astype('str').isin(closedOrders), f'{txnsType2}Status'] = 'Closed'
        orders[f'{txnsType2}No'] = orders[f'{txnsType2}No'].fillna('').astype('str')
        orderCloseDates[f'{txnsType2}No'] = orderCloseDates[f'{txnsType2}No'].fillna('').astype('str')
        orders = orders.merge(orderCloseDates, on = f'{txnsType2}No', how = 'left')
        if not orders.empty:
            orders.loc[orders[f'{txnsType2}Status']=='Open', 'CloseDate'] = pd.NaT
        else:
            orders['CloseDate'] = pd.NaT
        orders[f'{txnsType3}No'] = orders[f'{txnsType3}No'].fillna('').astype('str')
        customersORvendors[f'{txnsType3}No'] = customersORvendors[f'{txnsType3}No'].fillna('').astype('str')
        orders = orders.merge(customersORvendors[[f'{txnsType3}Id', f'{txnsType3}No', f'{txnsType3}Name']], on = f'{txnsType3}No', how = 'left')
        orders = orders[[f'{txnsType2}Id', f'{txnsType2}No', f'{txnsType2}Status', f'{txnsType2}Date', 'CloseDate', txnsType4, txnsType5, f'{txnsType3}Id', f'{txnsType3}No', f'{txnsType3}Name', 'ShipName', 'ShipCity', 'ShipState', 'ShipZip', 'subTotal', 'Total']].copy()
        orders = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = orders, df_name = 'orders', id_column = [f'{txnsType2}No'], additional_date_columns = [], zip_code_columns = ['ShipZip'], state_columns = ['ShipState'], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        orders = orders[~orders[f'{txnsType2}No'].str.upper().duplicated()]
        orders[f'{txnsType2}No'] = orders[f'{txnsType2}No'].fillna('').astype('str')
        ordersLines[f'{txnsType2}No'] = ordersLines[f'{txnsType2}No'].fillna('').astype('str')
        mismatched_orders = orders.merge(ordersLines, on=f'{txnsType2}No', how='inner', suffixes=('_ord', '_lin')).groupby(f'{txnsType2}No').agg({'subTotal':'max', 'Total_lin':'sum'}).reset_index()
        mismatched_orders = mismatched_orders[~np.isclose(mismatched_orders['subTotal'], mismatched_orders['Total_lin'], atol=0.1)]
        print(f"{mismatched_orders.shape[0]} orders total do not match orderline total")
        orders = orders[~orders[f'{txnsType2}No'].isin(mismatched_orders[f'{txnsType2}No'])]
        #-----------------------------------------------------------------------------------------------------------
        orders.drop(columns = 'Total', inplace=True)
        orders.rename(columns = {'subTotal':'Total'}, inplace = True)
        orders[f'{txnsType3}Id'] = orders[f'{txnsType3}No'].copy()
        #-----------------------------------------------------------------------------------------------------------
        #-------------------------------------
        orders = orders.drop_duplicates(subset=[f'{txnsType2}Id'])
        orders = orders.loc[orders[f'{txnsType2}Id'].notna() & (orders[f'{txnsType2}Id'].fillna('').astype('str').str.strip() != '')]
        orders['Company'] = companyName
        orders = orders[['Company'] + orders.columns[:-1].tolist()]

        #-------------------------------------
        ordersLines = ordersLines[ordersLines[f'{txnsType2}No'].isin(orders[f'{txnsType2}No'])]
        #-------------------------------------
        ordersLines, itemsCategoriesV3, item_df = enrich_and_classify_items(
            item_df, 
            companyName, 
            s3_client, 
            s3_bucket_name, 
            DBIA, 
            itemsCategoriesV3,
            ordersLines
        )
        #-----------------------------------------------------------------------------------------------------------
        orderTypes = ordersLines.merge(
                            itemsCategoriesV3[['index', 'ItemLevel2']] \
                            .rename(columns = {'index':'ItemId'}) \
                            .drop_duplicates(subset = 'ItemId'), on = 'ItemId'
                        ) \
                        .rename(columns={'ItemLevel2': f'{txnsType2}Type'}) \
                        .sort_values([f'{txnsType2}No', 'Total'], ascending=[True, False]) \
                        .groupby(f'{txnsType2}No').agg({f'{txnsType2}Type': 'first'}).reset_index()

        orders[f'{txnsType2}No'] = orders[f'{txnsType2}No'].fillna('').astype('str')
        orderTypes[f'{txnsType2}No'] = orderTypes[f'{txnsType2}No'].fillna('').astype('str')
        orders = orders.merge(orderTypes, on = f'{txnsType2}No', how = 'left')
        #-----------------------------------------------------------------------------------------------------------
        if companyName == 'KOMAR':  
            ordersLines = ordersLines.merge(orders[[f'{txnsType2}Id', f'{txnsType3}No']], on = f'{txnsType2}Id', how = 'left').rename(columns = {f'{txnsType3}No' : 'SerialNo'})
            ordersLines['SerialNo'] = ordersLines['SerialNo'].apply(
                lambda x: re.search(r'(?<!\d)(\d{4})(?!\d)', str(x)).group() if re.search(r'(?<!\d)(\d{4})(?!\d)', str(x)) else None
            )
            shipInstallDates = read_csv_from_s3(s3_client = s3_client, bucket_name = 'manual-db', object_key = 'serialNumberEngineeringReferenceMaster.csv')[['Serial Number', 'Installation Date']].rename(columns = {'Serial Number' : 'SerialNo', 'Ship Date' : 'ShipDate', 'Installation Date' : 'InstallDate'})
            shipInstallDates = shipInstallDates[~shipInstallDates.SerialNo.duplicated()]
            shipInstallDates.SerialNo = shipInstallDates.SerialNo.fillna('').astype('str')
            ordersLines.SerialNo = ordersLines.SerialNo.fillna('').astype('str')
            shipInstallDates['InstallDate'] = pd.to_datetime(shipInstallDates['InstallDate'], errors='coerce')
            ordersLines = ordersLines.merge(shipInstallDates, on = 'SerialNo', how = 'left')
        else:
            ordersLines['InstallDate'] = np.nan
        #-----------------------------------------------------------------------------------------------------------
        ordersLines.loc[
            (pd.to_datetime(ordersLines['ShipDate']) < start_date) |
            (pd.to_datetime(ordersLines['ShipDate']) > end_date),
            'ShipDate'
        ] = np.nan
        ordersLines.loc[
            (pd.to_datetime(ordersLines['InstallDate']) < start_date) |
            (pd.to_datetime(ordersLines['InstallDate']) > end_date),
            'InstallDate'
        ] = np.nan
        #-----------------------------------------------------------------------------------------------------------
        SalesOrderLinkedTxn[f'{txnsType2}No'] = SalesOrderLinkedTxn[f'{txnsType2}No'].fillna('').astype('str')
        ordersLines[f'{txnsType2}No'] = ordersLines[f'{txnsType2}No'].fillna('').astype('str')
        ordersLines = ordersLines.merge(SalesOrderLinkedTxn, on = f'{txnsType2}No', how = 'left')
        #-----------------------------------------------------------------------------------------------------------
        orders[f'{txnsType2}Id'] = orders[f'{txnsType2}Id'].fillna('').astype('str')
        ordersLines[f'{txnsType2}Id'] = ordersLines[f'{txnsType2}Id'].fillna('').astype('str')
        ordersLines = ordersLines.merge(orders[[f'{txnsType2}Id', f'{txnsType2}Status']], on = f'{txnsType2}Id', how = 'left').rename(columns={f'{txnsType2}Status':'ItemStatus'})
        ordersLines.loc[(ordersLines['ShipDate'].notna()), 'ItemStatus'] = 'SHIPPED'
        ordersLines.loc[(ordersLines['InstallDate'].notna()), 'ItemStatus'] = 'INSTALLED'
        ordersLines.loc[(ordersLines['InvoiceDate'].notna()), 'ItemStatus'] = 'INVOICED'
        #-----------------------------------------------------------------------------------------------------------
        ordersLines.rename(columns = {'CommonName':'ItemType'}, inplace = True)
        #-----------------------------------------------------------------------------------------------------------
    return orders, ordersLines, item_df


def process_s50_transactions(
    list_of_accounts,
    companyName,
    JrnlHdr,
    JrnlRow,
    employees,
    billToAdds,
    item,
    customer,
    start_date,
    end_date,
    s3_client,
    s3_bucket_name
):
    for txnsType in [
        ('Purchase'),
        ('Sales Invoice'),
        ('Cash Disbursements'),
        ('Cash Receipts'),
        ('General'),
    #     ('Sales Order'),
    #     ('Quotes'),
    #     ('Purchase Orders'),
    #     ('TempBelowZeroInvAdj'),
    #     ('Journal'),
    #     ('Inventory Adjustment'),
    #     ('Assembly Adjustments'),
    ]:
        print(txnsType)
        txnsLines = JrnlRow[JrnlRow['Journal']==txnsType].copy()
        txnsLines = txnsLines[
            (txnsLines['GLAcntNumber'].astype('str').isin(list_of_accounts['GLAcntNumber'].astype('str')))&\
            (pd.to_datetime(txnsLines['RowDate'], errors='coerce')>=start_date)&\
            (pd.to_datetime(txnsLines['RowDate'], errors='coerce')<=end_date)
        ].copy()
        
        txnNos = JrnlHdr[JrnlHdr['JrnlKey_Journal']==txnsType][['PostOrder', 'Reference']].copy()
        txnNos = txnNos[~txnNos['PostOrder'].duplicated() & ~txnNos['Reference'].duplicated()]
        txnNos.PostOrder = txnNos.PostOrder.astype('str')
        txnsLines.PostOrder = txnsLines.PostOrder.astype('str')
        txnsLines = txnsLines.merge(txnNos, on = 'PostOrder', how = 'left')
        txnsLines.rename(columns = {
            'PostOrder':'TransactionId',
            'Reference':'TransactionNo',
            'JrnlKey_Journal':'TransactionType',
            'ItemRecordNumber':'ItemId',
            'RowDescription':'ItemDescription',
            'Quantity':'Quantity',
            'UnitCost':'Rate',
            'Amount':'Total'
        }, inplace = True)    
        txnsLines['TransactionNo'] = txnsLines['TransactionNo'].astype('str')
        txnsLines['TransactionNo'] = txnsLines['TransactionNo'].apply(convert_to_int_or_keep)
        txnsLines = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txnsLines, df_name = 'txnsLines', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        txnsLines['Total'] = txnsLines['Total'].astype('str').str.replace(',', '').astype('float') * -1
        txnsLines['Quantity'] = txnsLines['Quantity'].astype('str').str.replace(',', '').astype('float')
        txnsLines = txnsLines[['TransactionId', 'TransactionNo', 'Account', 'ItemId', 'ItemDescription', 'Quantity', 'Rate', 'Total']]
        txnsLines[['Quantity', 'Rate', 'Total']] = txnsLines[['Quantity', 'Rate', 'Total']].fillna(0)
        txnsLines['Quantity'] = txnsLines['Quantity'].astype('float').round(2)
        txnsLines['Rate'] = txnsLines['Rate'].astype('float').round(2)
        txnsLines['Total'] = txnsLines['Total'].astype('float').round(2)   
        txnsLines.ItemId = txnsLines.ItemId.fillna('').astype('str')
        item.ItemId = item.ItemId.fillna('').astype('str')
        txnsLines = txnsLines.merge(item[['ItemId', 'ItemNo', 'ItemName']], on='ItemId', how='left')
        txnsLines.loc[txnsLines['ItemId']=='0', 'ItemId']='ITEM'
        txnsLines.loc[txnsLines['ItemId']=='0', 'ItemNo']='ITEM'
        txnsLines.loc[txnsLines['ItemId']=='0', 'ItemName']='ITEM'
        txnsLines = txnsLines[['TransactionId', 'TransactionNo', 'Account', 'ItemId', 'ItemNo', 'ItemName', 'ItemDescription', 'Quantity', 'Rate', 'Total']]        
        txnsLines['Company'] = companyName
        txnsLines = txnsLines[['Company'] + txnsLines.columns[:-1].tolist()]
        txns_df = JrnlHdr[JrnlHdr['JrnlKey_Journal']==txnsType].copy()
        txns_df = txns_df[
            (pd.to_datetime(txns_df['TransactionDate'])>=start_date)&\
            (pd.to_datetime(txns_df['TransactionDate'])<=end_date)
        ].copy()
        EmpNames = employees[['EmpRecordNumber', 'EmployeeName']].copy()
        EmpNames = EmpNames[~EmpNames['EmpRecordNumber'].duplicated()]
        EmpNames.EmpRecordNumber = EmpNames.EmpRecordNumber.astype('str')
        txns_df.EmpRecordNumber = txns_df.EmpRecordNumber.astype('str')
        txns_df = txns_df.merge(EmpNames, on = 'EmpRecordNumber', how = 'left')
        billToAdds.CustVendId = billToAdds.CustVendId.astype('str')
        txns_df.CustVendId = txns_df.CustVendId.astype('str')
        txns_df = txns_df.merge(billToAdds, on = 'CustVendId', how = 'left')
        txns_df.rename(columns = {
            'PostOrder':'TransactionId',
            'JrnlKey_Journal':'TransactionType',
            'Reference':'TransactionNo', 
            'TransactionDate':'TransactionDate',
            'POSOisClosed':'TransactionStatus',
            'EmployeeName':'SalesRepID',
            'CustVendId':'CustId',
            'ShipToName':'ShipName',
            'ShipToCity':'ShipCity',
            'ShipToState':'ShipState',
            'ShipToZIP':'ShipZip',
            'MainAmount':'Total',
        }, inplace = True)
        txns_df['TransactionNo'] = txns_df['TransactionNo'].astype('str')
        txns_df['TransactionNo'] = txns_df['TransactionNo'].apply(convert_to_int_or_keep)
        txns_df = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns_df, df_name = 'txns_df', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        txns_df = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns_df, df_name = 'txns_df', id_column = ['TransactionId'], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        txns_df['Total'] = txns_df['Total'].astype('str').str.replace(',', '').astype('float')
        txns_df['TransactionStatus'] = txns_df['TransactionStatus'].astype('str').replace({'1': 'Closed', '0': 'Open'})
        txns_df.SalesRepID = txns_df.SalesRepID.fillna('').str.split(':').str[-1]
        txns_df['TransactionStatus'] = np.where(txns_df['CompletedDate'].isna(), 'Open', 'Closed')
        txns_df.rename(columns = {
            'INV_POSOOrderNumber':'OrderNo',
            'PurchOrder':'CustPo',
        }, inplace = True)
        txns_df.drop(columns = ['Total'], inplace = True)
        txns_df['TransactionId'] = txns_df['TransactionId'].astype('str')
        txnsLines['TransactionId'] = txnsLines['TransactionId'].astype('str')
        txns_df = txns_df.merge(
            txnsLines.groupby('TransactionId').agg(Total = ('Total', 'sum')).reset_index(),
            on='TransactionId',
        )
        txns_df['subTotal'] = txns_df['Total']
        if txnsType == 'General':
            generalJournalLines = txnsLines.copy()
            generalJournal = txns_df.copy()
        elif txnsType == 'Sales Invoice':
            invoicesLines = txnsLines.copy()
            invoices = txns_df.copy()
        elif txnsType == 'Purchase':
            billsLines = txnsLines.copy()
            bills = txns_df.copy()
        elif txnsType == 'Cash Receipts':
            depositsLines = txnsLines.copy()
            deposits = txns_df.copy()
        elif txnsType == 'Cash Disbursements':
            paymentsLines = txnsLines.copy()
            payments = txns_df.copy()   
    #-----------------------------------------------------------------------------------------------------------
    txns = pd.concat([invoices, generalJournal, bills, deposits, payments], ignore_index=True)  
    txnsLines = pd.concat([invoicesLines, generalJournalLines, billsLines, depositsLines, paymentsLines], ignore_index=True)
    #-----------------------------------------------------------------------------------------------------------
    txns.CustId = txns.CustId.astype('str')
    customer.CustId = customer.CustId.astype('str')
    txns = txns.merge(customer[['CustId', 'CustNo', 'CustName']], on = 'CustId', how = 'left').copy()
    txns = txns[['OrderNo', 'TransactionId', 'TransactionStatus', 'TransactionNo', 'TransactionType', 'TransactionDate', 'SalesRepID', 'CustPo', 'CustId', 'CustNo', 'CustName', 'ShipName', 'ShipCity', 'ShipState', 'ShipZip', 'BillName', 'BillCity', 'BillState', 'BillZip', 'subTotal', 'Total']].copy()
    txns = txns[~txns['TransactionId'].astype('str').str.upper().duplicated()]
    txns['Company'] = companyName
    txns = txns[['Company'] + txns.columns[:-1].tolist()]
    txns.TransactionId = txns.TransactionId.astype('str')
    txnsLines.TransactionId = txnsLines.TransactionId.astype('str')
    mismatched_txns = txns.merge(txnsLines, on='TransactionId', how='inner', suffixes=('_ord', '_lin')).groupby('TransactionId').agg({'subTotal':'max', 'Total_lin':'sum'}).reset_index()
    mismatched_txns = mismatched_txns[~np.isclose(mismatched_txns['subTotal'], mismatched_txns['Total_lin'], atol=0.1)]
    print(f"{mismatched_txns.shape[0]} txns Total do not match orderline Total")
    txns = txns[~txns['TransactionId'].isin(mismatched_txns['TransactionId'])]
    txns = txns[txns['TransactionId'].isin(txnsLines['TransactionId'])]
    txnsLines = txnsLines[txnsLines['TransactionId'].isin(txns['TransactionId'])]
    return txns, txnsLines


def process_s50_orders(
    companyName,
    JrnlHdr,
    JrnlRow,
    employees,
    billToAdds,
    item,
    item_df,
    customersORvendors,
    start_date,
    end_date,
    s3_client,
    s3_bucket_name,
    txnsType,
    txnsType2,
    txnsType3,
    txnsType4,
    txnsType5,
    rename_map,
    orderCloseDates,
    DBIA,
    itemsCategoriesV3
):
    #------------------------------------------------------------
    prompt = f'Step5: {txnsType}...'
    print(prompt)
    write_file('log.txt' , f"{print_date_time()}\t\t{prompt}")
    #------------------------------------------------------------

    txns_df = JrnlHdr[JrnlHdr['JrnlKey_Journal']==txnsType].copy()    
    EmpNames = employees[['EmpRecordNumber', 'EmployeeName']].copy()
    EmpNames = EmpNames[~EmpNames['EmpRecordNumber'].duplicated()]
    EmpNames.EmpRecordNumber = EmpNames.EmpRecordNumber.astype('str')
    txns_df.EmpRecordNumber = txns_df.EmpRecordNumber.astype('str')
    txns_df = txns_df.merge(EmpNames, on = 'EmpRecordNumber', how = 'left')    
    billToAdds.CustVendId = billToAdds.CustVendId.astype('str')
    txns_df.CustVendId = txns_df.CustVendId.astype('str')
    txns_df = txns_df.merge(billToAdds, on = 'CustVendId', how = 'left')    
    txns_df.rename(columns = {
        'PostOrder':f'{txnsType2}Id',
        'Reference':f'{txnsType2}No', 
        'TransactionDate':f'{txnsType2}Date',
        'POSOisClosed':f'{txnsType2}Status',
        'EmployeeName':txnsType4,
        'CustVendId':f'{txnsType3}Id',
        'ShipToName':'ShipName',
        'ShipToCity':'ShipCity',
        'ShipToState':'ShipState',
        'ShipToZIP':'ShipZip',
        'MainAmount':'Total',
    }, inplace = True)    
    txns_df[f'{txnsType2}No'] = txns_df[f'{txnsType2}No'].astype('str').apply(convert_to_int_or_keep)
    txns_df = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns_df, df_name = 'txns_df', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
    txns_df = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txns_df, df_name = 'txns_df', id_column = [f'{txnsType2}Id'], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
    
    if txnsType == 'Purchase Orders':
        txns_df['Total'] = txns_df['Total'].astype('str').str.replace(',', '').astype('float')*-1
    else:
        txns_df['Total'] = txns_df['Total'].astype('str').str.replace(',', '').astype('float')

    txns_df[f'{txnsType2}Status'] = txns_df[f'{txnsType2}Status'].astype('str').replace({'1': 'Closed', '0': 'Open'})
    txns_df[txnsType4] = txns_df[txnsType4].fillna('').str.split(':').str[-1]    
    txns_df.drop(columns=['ShipDate'], inplace=True)
    txns_df.rename(columns = rename_map, inplace = True)
    txns_df = txns_df[[f'{txnsType2}Id', f'{txnsType2}No', f'{txnsType2}Date', f'{txnsType2}Status', 'ShipDate', txnsType4, txnsType5 , f'{txnsType3}Id', 'BillName', 'BillCity', 'BillState', 'BillZip', 'ShipName', 'ShipCity', 'ShipState', 'ShipZip', 'Total']].copy()
    txns_df = txns_df[pd.to_datetime(txns_df[f'{txnsType2}Date']) > start_date]
    txns_df = txns_df[pd.to_datetime(txns_df[f'{txnsType2}Date']) < end_date]
    orders = txns_df.copy()

    #### Lines
    txnsLines = JrnlRow[JrnlRow['Journal']==txnsType].copy()
    txnNos = JrnlHdr[JrnlHdr['JrnlKey_Journal']==txnsType][['PostOrder', 'Reference']].copy()
    txnNos = txnNos[~txnNos['PostOrder'].duplicated() & ~txnNos['Reference'].duplicated()]
    txnNos.PostOrder = txnNos.PostOrder.astype('str')
    txnsLines.PostOrder = txnsLines.PostOrder.astype('str')
    txnsLines = txnsLines.merge(txnNos, on = 'PostOrder', how = 'left')
    txnsLines.rename(columns = {
        'PostOrder':f'{txnsType2}Id',
        'Reference':f'{txnsType2}No',
        'ItemRecordNumber':'ItemId',
        'RowDescription':'ItemDescription',
        'Quantity':'Quantity',
        'UnitCost':'Rate',
        'Amount':'Total'
    }, inplace = True)    
    txnsLines = txnsLines[~txnsLines[f'{txnsType2}No'].isna()].copy()
    txnsLines[f'{txnsType2}No'] = txnsLines[f'{txnsType2}No'].astype('str')
    txnsLines[f'{txnsType2}No'] = txnsLines[f'{txnsType2}No'].apply(convert_to_int_or_keep)
    txnsLines = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = txnsLines, df_name = 'txnsLines', id_column = [], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
    
    if txnsType == 'Purchase Orders':
        txnsLines['Total'] = txnsLines['Total'].astype('str').str.replace(',', '').astype('float')
    else:
        txnsLines['Total'] = txnsLines['Total'].astype('str').str.replace(',', '').astype('float')*-1
        
    txnsLines['Quantity'] = txnsLines['Quantity'].astype('str').str.replace(',', '').astype('float')
    txnsLines = txnsLines[[f'{txnsType2}Id', f'{txnsType2}No', 'ItemId', 'ItemDescription', 'Quantity', 'Rate', 'Total']]
    txnsLines[['Quantity', 'Rate', 'Total']] = txnsLines[['Quantity', 'Rate', 'Total']].fillna(0)
    txnsLines['Quantity'] = txnsLines['Quantity'].astype('float').round(2)
    txnsLines['Rate'] = txnsLines['Rate'].astype('float').round(2)
    txnsLines['Total'] = txnsLines['Total'].astype('float').round(2)    
    txnsLines.ItemId = txnsLines.ItemId.fillna('').astype('str')
    item.ItemId = item.ItemId.fillna('').astype('str')
    txnsLines = txnsLines.merge(item[['ItemId', 'ItemNo', 'ItemName']], on='ItemId', how='left')  
    txnsLines = txnsLines[[f'{txnsType2}Id', f'{txnsType2}No', 'ItemId', 'ItemNo', 'ItemName', 'ItemDescription', 'Quantity', 'Rate', 'Total']]
    txnsLines['Company'] = companyName
    txnsLines = txnsLines[['Company'] + txnsLines.columns[:-1].tolist()]
    ordersLines = txnsLines.copy() 
    orders[f'{txnsType2}Id'] = orders[f'{txnsType2}Id'].astype('str')
    ordersLines[f'{txnsType2}Id'] = ordersLines[f'{txnsType2}Id'].astype('str')
    ordersLines = ordersLines.merge(orders[[f'{txnsType2}Id','ShipDate']], on = f'{txnsType2}Id', how = 'left')
    orders.drop(columns=['ShipDate'], inplace=True)

    orders[f'{txnsType2}No'] = orders[f'{txnsType2}No'].astype('str')
    orderCloseDates[f'{txnsType2}No'] = orderCloseDates[f'{txnsType2}No'].astype('str')
    orders = orders.merge(orderCloseDates, on = f'{txnsType2}No', how = 'left')
    if not orders.empty:
        orders.loc[orders[f'{txnsType2}Status']=='Open', 'CloseDate'] = pd.NaT
    else:
        orders['CloseDate'] = pd.NaT

    #-------------------------------------
    ordersLines, itemsCategoriesV3, item_df = enrich_and_classify_items(
        item_df, 
        companyName, 
        s3_client, 
        s3_bucket_name, 
        DBIA, 
        itemsCategoriesV3,
        ordersLines
    )
    #-----------------------------------------------------------------------------------------------------------
    orderTypes = ordersLines.merge(
                        itemsCategoriesV3[['index', 'ItemLevel2']] \
                        .rename(columns = {'index':'ItemId'}) \
                        .drop_duplicates(subset = 'ItemId'), on = 'ItemId'
                    ) \
                    .rename(columns={'ItemLevel2': f'{txnsType2}Type'}) \
                    .sort_values([f'{txnsType2}No', 'Total'], ascending=[True, False]) \
                    .groupby(f'{txnsType2}No').agg({f'{txnsType2}Type': 'first'}).reset_index()

    orders[f'{txnsType2}No'] = orders[f'{txnsType2}No'].astype('str')
    orderTypes[f'{txnsType2}No'] = orderTypes[f'{txnsType2}No'].astype('str')
    orders = orders.merge(orderTypes, on = f'{txnsType2}No', how = 'left')
    orders[f'{txnsType3}Id'] = orders[f'{txnsType3}Id'].astype('str')
    customersORvendors[f'{txnsType3}Id'] = customersORvendors[f'{txnsType3}Id'].astype('str')
    orders = orders.merge(customersORvendors[[f'{txnsType3}Id', f'{txnsType3}No', f'{txnsType3}Name']], on = f'{txnsType3}Id', how = 'left')
    orders = orders[[f'{txnsType2}Id', f'{txnsType2}No', f'{txnsType2}Type', f'{txnsType2}Status', f'{txnsType2}Date', 'CloseDate', txnsType4, txnsType5, f'{txnsType3}Id', f'{txnsType3}No', f'{txnsType3}Name', 'ShipName', 'ShipCity', 'ShipState', 'ShipZip', 'Total']].copy()
    orders['Company'] = companyName
    orders = orders[['Company'] + orders.columns[:-1].tolist()]
    orders = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = orders, df_name = 'orders', id_column = [f'{txnsType2}Id'], additional_date_columns = [], zip_code_columns = ['ShipZip'], state_columns = ['ShipState'], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
    orders = orders[~orders[f'{txnsType2}Id'].str.upper().duplicated()]
    orders[f'{txnsType2}Id'] = orders[f'{txnsType2}Id'].astype('str')
    ordersLines[f'{txnsType2}Id'] = ordersLines[f'{txnsType2}Id'].astype('str')
    mismatched_orders = orders.merge(ordersLines, on=f'{txnsType2}Id', how='inner', suffixes=('_ord', '_lin')).groupby(f'{txnsType2}Id').agg({'Total_ord':'max', 'Total_lin':'sum'}).reset_index()
    mismatched_orders = mismatched_orders[~np.isclose(mismatched_orders['Total_ord'], mismatched_orders['Total_lin'], atol=0.1)]
    print(f"{mismatched_orders.shape[0]} orders total do not match orderline total")
    orders = orders[~orders[f'{txnsType2}Id'].isin(mismatched_orders[f'{txnsType2}Id'])]
    orders['CustNo'] = orders[f'{txnsType3}Id'].copy()
    #-------------------------------------
    orders = orders.drop_duplicates(subset=[f'{txnsType2}Id'])
    orders = orders.loc[orders[f'{txnsType2}Id'].notna() & (orders[f'{txnsType2}Id'].astype('str').str.strip() != '')]
    return orders, ordersLines, item_df

    
def get_reporting_period_label(latest_date):
    date_diff_days = (datetime.today().date() - latest_date.date()).days
    period_thresholds = {
        7: '7 days',
        30: '1 month',
        90: '3 months',
        180: '6 months',
        365: '1 year',
        3 * 365: '3 years',
        5 * 365: '5 years',
        10 * 365: '10 years',
    }
    for threshold in sorted(period_thresholds):
        if date_diff_days < threshold:
            return period_thresholds[threshold]
    return 'All time'


def get_exchange_rates(
    from_currency,
    to_currency,
    frequency='daily',
    reporting_period='7 days',
    max_attempts = 10
):
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
    driver.get('https://www.ofx.com/en-us/forex-news/historical-exchange-rates/')
    wait = WebDriverWait(driver, 120)

    def find_and_act(element_id, action='click', text=None, max_attempts=max_attempts):
        attempts = 0
        while attempts < max_attempts:
            try:
                element = wait.until(EC.presence_of_element_located((By.ID, element_id)))
                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                time.sleep(0.5)
                if action == 'click':
                    element.click()
                elif action == 'send_keys' and text is not None:
                    element.send_keys(text)
                elif action == 'enter':
                    element.send_keys(Keys.ENTER)
                return True

            except Exception as e:
                print(f"[INFO] Attempt {attempts+1}/{max_attempts}: Element '{element_id}' not interactable... Retrying...")
                attempts += 1
                time.sleep(1)
        print(f"[ERROR] Failed to interact with element '{element_id}' after {max_attempts} attempts.")
        return False

    find_and_act("react-select-ofx-historical-rates-select-from-field-input", action='send_keys', text=from_currency)
    find_and_act("react-select-ofx-historical-rates-select-from-field-input", action='enter')
    find_and_act("react-select-ofx-historical-rates-select-to-field-input", action='send_keys', text=to_currency)
    find_and_act("react-select-ofx-historical-rates-select-to-field-input", action='enter')
    find_and_act(f"choice_frequency_{frequency}")
    find_and_act("react-select-ofx-historical-rates-select-period-field-input", action='send_keys', text=reporting_period)
    find_and_act("react-select-ofx-historical-rates-select-period-field-input", action='enter')
    table = driver.find_element(By.CLASS_NAME, "ofx-historical-rates__table")
    old_table_html = table.get_attribute("innerHTML")
    retrieve_button = wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "button.wp-block-button__link")
    ))
    retrieve_button.click()
    time.sleep(1)
    wait.until(lambda d: d.find_element(By.CLASS_NAME, "ofx-historical-rates__table").get_attribute("innerHTML") != old_table_html)
    table = driver.find_element(By.CLASS_NAME, "ofx-historical-rates__table")
    rows = table.find_elements(By.XPATH, ".//tbody/tr")
    data = []
    for row in tqdm(rows, desc="Fetching rows", unit="row"):
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) == 2:
            date = cols[0].text.strip()
            rate = cols[1].text.strip()
            data.append([date, rate])
    driver.quit()
    df = pd.DataFrame(data, columns=["Date", f"{from_currency}/{to_currency}"])
    df = df.loc[df['Date']!='Average']
    df['Date']= pd.to_datetime(df['Date'], errors='coerce')
    return df


def process_exchange_rates(
    from_currency,
    to_currency,
    s3_client,
    bucket_name,
    object_key,
    aws_region=None,
    CreateS3Bucket=True
):

    try:
        df = read_csv_from_s3(s3_client = s3_client, bucket_name = bucket_name, object_key = object_key)
        df['Date']= pd.to_datetime(df['Date'], errors='coerce')
        reporting_period = get_reporting_period_label(df['Date'].max())
        dfNew = get_exchange_rates(
            from_currency=from_currency,
            to_currency=to_currency,
            frequency='daily',
            reporting_period=reporting_period
        )
        dfNew['Date'] = pd.to_datetime(dfNew['Date'], errors='coerce')
        dfNew = dfNew.loc[dfNew.Date.notna()]
        if not dfNew.empty:
            df = pd.concat([df[df['Date'] < dfNew['Date'].min()], dfNew], ignore_index=True)
    except:
        df = get_exchange_rates(
            from_currency=from_currency,
            to_currency=to_currency,
            frequency='daily',
            reporting_period='All time'
        )

    upload_to_s3(s3_client = s3_client, data = df, bucket_name = bucket_name, object_key = object_key, aws_region=aws_region)
    df[f'{from_currency}/{to_currency}'] = pd.to_numeric(df[f'{from_currency}/{to_currency}'], errors='coerce')
    dfAvg = df[df['Date'].dt.day != 1].groupby([df['Date'].dt.year, df['Date'].dt.month]).agg({f'{from_currency}/{to_currency}': 'mean'})
    dfAvg.index.set_names(['Year', 'Month'], inplace=True)
    return dfAvg


def load_data_via_query(
        sql_query,
        source_type,
        connection_string=None,
        project_id=None,
        credentials=None,
        chunksize=1000,
        file_path=None,
        encoding='utf-8'
):
    print(f"\tRunning {sql_query}")
    if source_type == "mssql":
        if not connection_string:
            raise ValueError("connection_string is required for MSSQL source.")
        chunks = []
        if not file_path:
            try:
                with pyodbc.connect(connection_string) as conn:
                    total_rows = pd.read_sql_query("SELECT COUNT(*) FROM ({}) subquery".format(sql_query), conn).iloc[0, 0]
                    total_chunks = (total_rows // chunksize) + (total_rows % chunksize > 0)
                    for chunk in tqdm(pd.read_sql_query(sql_query, conn, chunksize=chunksize), total=total_chunks):
                        chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
                df.columns = df.columns.str.title()
                return df
            except:
                with pyodbc.connect(connection_string) as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM ({sql_query}) AS subquery")
                    total_rows = cursor.fetchone()[0]
                    total_chunks = (total_rows // chunksize) + (total_rows % chunksize > 0)
                    cursor.execute(sql_query)
                    with tqdm(total=total_chunks, desc="Fetching rows") as pbar:
                        while True:
                            chunk = cursor.fetchmany(chunksize)
                            if not chunk:
                                break
                            chunks.extend(chunk)
                            pbar.update(1)
                        columns = [column[0] for column in cursor.description]
                        data = [dict(zip(columns, row)) for row in chunks]
                        return data
        else:
            with pyodbc.connect(connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM ({sql_query}) AS subquery")
                total_rows = cursor.fetchone()[0]
                total_chunks = (total_rows // chunksize) + (total_rows % chunksize > 0)
                cursor.execute(sql_query)
                with tqdm(total=total_chunks, desc="Fetching rows") as pbar:
                        if os.path.exists(file_path):
                            os.remove(file_path)
                        with open(file_path, 'w', newline='', encoding=encoding) as f:
                            writer = csv.writer(f)
                            first_chunk = True
                            while True:
                                chunk = cursor.fetchmany(chunksize)
                                if not chunk:
                                    break
                                if first_chunk:
                                    columns = [column[0] for column in cursor.description]
                                    writer.writerow(columns)
                                    first_chunk = False
                                for row in chunk:
                                    writer.writerow([str(value) for value in row])
                                pbar.update(1)

    elif source_type == "bigquery":
        if not (project_id and credentials):
            raise ValueError("project_id and credentials are required for BigQuery source.")
        df = pandas_gbq.read_gbq(sql_query, project_id=project_id, credentials=credentials)
        df.columns = df.columns.str.title()
        return df

    elif source_type == "qodbc":
        if not connection_string:
            raise ValueError("connection_string is required for QODBC source.")
        conn = pyodbc.connect(connection_string, autocommit=True)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [col[0] for col in cursor.description]
        chunks = []
        first_chunk = True
        with tqdm(desc="Fetching rows (QODBC – no row count)", unit="chunk") as pbar:
            while True:
                rows = cursor.fetchmany(chunksize)
                if not rows:
                    break
                if file_path:
                    if first_chunk:
                        if os.path.exists(file_path):
                            os.remove(file_path)
                        with open(file_path, "w", newline='', encoding=encoding) as f:
                            writer = csv.writer(f)
                            writer.writerow(columns)
                        first_chunk = False
                    with open(file_path, "a", newline='', encoding=encoding) as f:
                        writer = csv.writer(f)
                        for r in rows:
                            writer.writerow([str(v) for v in r])
                else:
                    for r in rows:
                        chunks.append(dict(zip(columns, r)))
                    first_chunk = False
                pbar.update(1)
        cursor.close()
        conn.close()
        if not file_path:
            df = pd.DataFrame(chunks, columns=columns)
            df.columns = df.columns.str.title()
            return df

    else:
        raise ValueError("source_type must be 'mssql', 'bigquery', or 'qodbc'")

def upload_to_s3(
    data,
    bucket_name,
    object_key,
    s3_client,
    CreateS3Bucket=False,
    aws_region=None,
    chunk_size=5 * 1024 * 1024,
    file_path=None,
    encoding='utf-8'
):

    if CreateS3Bucket:
        try:
            buckets = s3_client.list_buckets()["Buckets"]
            buckets = [bucket['Name'] for bucket in buckets]
            if bucket_name not in buckets:
                s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': aws_region})
                prompt = f'{print_date_time()}\t\t[SUCCESS] Bucket "{bucket_name}" created!'
                print(prompt)
                write_file('log.txt' , f"{prompt}")
            else:
                prompt = f'{print_date_time()}\t\t[INFO] Bucket "{bucket_name}" already exists.'
                print(prompt)
                write_file('log.txt' , f"{prompt}")
        except Exception as e:
            prompt = f'{print_date_time()}\t\t[ERROR] Failed to create bucket "{bucket_name}". Error: {str(e)}.'
            print(prompt)
            write_file('log.txt' , f"{prompt}")
    if not file_path:
        clean_data = data.copy()
        for idx, dtype in enumerate(clean_data.dtypes):
            if dtype == 'object' or dtype.name == 'string':
                clean_data.iloc[:, idx] = (
                    clean_data.iloc[:, idx]
                    .fillna('')
                    .apply(lambda x: x.hex() if isinstance(x, bytes) else str(x))
                    .astype('str')
                    .str.replace(r'\r\n|\r|\n', ' ', regex=True)
                    .str.replace(r'\\n', ' ', regex=True)
                    .str.replace(r'\\', ' ', regex=True)
                )
        csv_buffer = io.StringIO()
        clean_data.to_csv(csv_buffer, index=False, sep=',', quotechar='"', quoting=csv.QUOTE_ALL, escapechar='\\', encoding=encoding)
        csv_buffer.seek(0)
        data_size = len(csv_buffer.getvalue())
        with tqdm(total=data_size, unit='B', unit_scale=True, desc=f'Uploading "{object_key}" to S3') as progress:
            def callback(bytes_transferred):
                progress.update(bytes_transferred)
            bytes_buffer = io.BytesIO(csv_buffer.getvalue().encode())
            s3_client.upload_fileobj(
                Fileobj=bytes_buffer,
                Bucket=bucket_name,
                Key=object_key,
                Callback=callback
        )
    else:
        multipart_upload = s3_client.create_multipart_upload(Bucket=bucket_name, Key=object_key)
        parts = []
        part_number = 1
        total_bytes_uploaded = 0
        file_size = os.path.getsize(file_path)
        progress = tqdm(total=file_size, unit='MB', desc=f'Uploading "{object_key}" to S3')
        def upload_part(buffer, part_number):
            nonlocal total_bytes_uploaded
            buffer.seek(0)
            response = s3_client.upload_part(
                Bucket=bucket_name,
                Key=object_key,
                PartNumber=part_number,
                UploadId=multipart_upload['UploadId'],
                Body=buffer
            )
            uploaded_size = buffer.tell()
            total_bytes_uploaded += uploaded_size
            progress.update(uploaded_size)
            return {
                'PartNumber': part_number,
                'ETag': response['ETag']
            }
        with open(file_path, 'r', encoding=encoding) as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer, quoting=csv.QUOTE_ALL)
            writer.writerow(headers)
            for row in csv_reader:
                writer.writerow(row)
                if csv_buffer.tell() >= chunk_size:
                    part = upload_part(io.BytesIO(csv_buffer.getvalue().encode()), part_number)
                    parts.append(part)
                    part_number += 1
                    csv_buffer = io.StringIO()
                    writer = csv.writer(csv_buffer, quoting=csv.QUOTE_ALL)
                    writer.writerow(headers)
            if csv_buffer.tell() > 0:
                part = upload_part(io.BytesIO(csv_buffer.getvalue().encode()), part_number)
                parts.append(part)
        progress.close()
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=object_key,
            UploadId=multipart_upload['UploadId'],
            MultipartUpload={'Parts': parts}
        )


def process_data_to_s3(
    tables,
    s3_client,
    bucket_name,
    source_type,
    connection_string=None,
    project_id=None,
    credentials=None,
    max_retries=3,
    CreateS3Bucket=False,
    aws_region=None,
    file_path=None,
    encoding='utf-8'
):
    for table, sql_query in tables.items():
        for attempt in range(max_retries):
            try:
                if not file_path:
                    df = load_data_via_query(
                        sql_query=sql_query,
                        source_type=source_type,
                        connection_string=connection_string,
                        project_id=project_id,
                        credentials=credentials
                    )
                else:
                    load_data_via_query(
                        sql_query=sql_query,
                        source_type=source_type,
                        connection_string=connection_string,
                        project_id=project_id,
                        credentials=credentials,
                        file_path=file_path,
                        encoding=encoding
                    )
                prompt = f'{print_date_time()}\t\t[SUCCESS] Table "{table}" retrieved from {source_type} !'
                print(prompt)
                write_file('log.txt' , f"{prompt}")
                break
            except Exception as e:
                prompt = f'{print_date_time()}\t\t[ERROR] Failed to retrieve table "{table}". Error: {str(e)}. Retry {attempt + 1}/{max_retries} in 1 minute...'
                print(prompt)
                write_file('log.txt' , f"{prompt}")
                time.sleep(60)
        else:
            prompt = f'{print_date_time()}\t\t[ERROR] All retries failed for table "{table}". Skipping upload.'
            print(prompt)
            write_file('log.txt' , f"{prompt}")
            continue
        object_key = table + '.csv'
        try:
            # prompt = f'{print_date_time()}\t\t[INFO] "{object_key}" table is empty and was not loaded to S3 "{bucket_name}" bucket !'
            if not file_path:
                # if df.empty:
                #     print(prompt)
                #     write_file('log.txt' , f"{prompt}")
                #     continue
                upload_to_s3(
                    data=df,
                    bucket_name=bucket_name,
                    object_key=object_key,
                    s3_client=s3_client,
                    CreateS3Bucket=CreateS3Bucket,
                    aws_region=aws_region,
                    encoding=encoding
                )
            else:
                if not os.path.getsize(file_path):
                    print(prompt)
                    write_file('log.txt' , f"{prompt}")
                    continue
                upload_to_s3(
                    data=None,
                    bucket_name=bucket_name,
                    object_key=object_key,
                    s3_client=s3_client,
                    CreateS3Bucket=CreateS3Bucket,
                    aws_region=aws_region,
                    file_path = file_path,
                    encoding=encoding
                )
            prompt = f'{print_date_time()}\t\t[SUCCESS] "{object_key}" table is loaded to S3 "{bucket_name}" bucket !'
            print(prompt)
            write_file('log.txt' , f"{prompt}")
        except Exception as e:
            prompt = f'{print_date_time()}\t\t[ERROR] Failed to load table "{object_key}" to S3 bucket "{bucket_name}". Error: {str(e)}'
            print(prompt)
            write_file('log.txt' , f"{prompt}")
        if 'df' in locals():
            del df
        gc.collect()


def generate_open_cases_df(
    df,
    min_date,
    openDateCol,
    closeDateCol,
    idCol,
    timezone,
    statusCol=None,
    excludeStatusList=None,
    quantile_values=None,
    n=5
):
    df = df[df[openDateCol] >= min_date].copy()
    df[idCol] = df[idCol].fillna('').astype('str').str.upper().str.strip().apply(convert_to_int_or_keep)
    if excludeStatusList:
        df = df[~df[statusCol].isin(excludeStatusList)].copy()
    open_df = pd.DataFrame()
    max_date = pd.to_datetime(datetime.now(timezone).date())
    current_date = min_date
    while current_date <= max_date:
        active_df = df[
            (df[openDateCol] <= current_date) &
            ((df[closeDateCol] > current_date) | df[closeDateCol].isnull())
        ].copy()
        active_df = active_df[~active_df[idCol].duplicated()]
        active_df['Age'] = (current_date - active_df[openDateCol]).dt.days
        active_df['Date'] = current_date
        open_df = pd.concat([open_df, active_df[
            ['Date', 'Age', idCol]
        ]], ignore_index=True)
        current_date += pd.Timedelta(days=1)
    if not quantile_values:
        quantile_values = [int(open_df['Age'].quantile(i/n)) for i in range(n+1)]
    print(quantile_values)
    open_df['Age Group'] = open_df['Age'].apply(lambda x: group(x, quantile_values))
    write_file('log.txt' , f"{print_date_time()}\t\tGenerated open cases dataframe successfully!")
    return open_df


def train_and_predict(
    labeled_df,
    unlabeled_df,
    input_cols,
    target_cols
):
    # 1. Combine input text columns
    labeled_df['combined_text'] = labeled_df[input_cols].astype('str').agg(' '.join, axis=1)

    # 2. Encode target columns
    output_encoders = {}
    Y_encoded = pd.DataFrame()
    for col in target_cols:
        le = LabelEncoder()
        Y_encoded[col] = le.fit_transform(labeled_df[col].astype('str'))
        output_encoders[col] = le

    # 3. Train-test split
    X = labeled_df['combined_text']
    Y = Y_encoded
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

    # 4. TF-IDF vectorization
    tfidf = TfidfVectorizer(max_features=500)
    X_train_tfidf = tfidf.fit_transform(X_train)
    X_test_tfidf = tfidf.transform(X_test)

    # 5. Train model
    model = MultiOutputClassifier(RandomForestClassifier(random_state=42))
    model.fit(X_train_tfidf, Y_train)

    # 6. Evaluate model
    y_pred_array = model.predict(X_test_tfidf)
    y_pred = pd.DataFrame(y_pred_array, columns=Y_test.columns, index=Y_test.index)

    Y_test_decoded = Y_test.copy()
    for col in target_cols:
        y_pred[col] = output_encoders[col].inverse_transform(y_pred[col])
        Y_test_decoded[col] = output_encoders[col].inverse_transform(Y_test[col])

    print("Per-column accuracy:")
    for col in target_cols:
        acc = accuracy_score(Y_test_decoded[col], y_pred[col])
        print(f"{col}: {acc:.4f}")

    exact_match_acc = np.mean(np.all(y_pred.values == Y_test_decoded.values, axis=1))
    print(f"Exact match accuracy: {exact_match_acc:.4f}")

    # 7. Predict on new dataset
    unlabeled_df ['combined_text'] = unlabeled_df [input_cols].astype('str').agg(' '.join, axis=1)
    X_new_tfidf = tfidf.transform(unlabeled_df ['combined_text'])

    preds_array = model.predict(X_new_tfidf)
    predicted_targets_df = pd.DataFrame(preds_array, columns=target_cols)
    for col in target_cols:
        predicted_targets_df[col] = output_encoders[col].inverse_transform(predicted_targets_df[col])

    final_predictions = pd.concat([
        unlabeled_df [input_cols].reset_index(drop=True),
        predicted_targets_df.reset_index(drop=True)
    ], axis=1)
    return final_predictions


def impute_by_group(df, group_col, target_col, method='median', mask=None):

    if mask is not None:
        df.loc[mask, target_col] = df.loc[mask, target_col].replace(0, np.nan)
    else:
        df[target_col] = df[target_col].replace(0, np.nan)
    group_stat = df.groupby(group_col)[target_col].transform(method)
    if mask is not None:
        df.loc[mask, target_col] = df.loc[mask, target_col].fillna(group_stat[mask])
        df.loc[mask, target_col] = df.loc[mask, target_col].fillna(df[target_col].agg(method))
    else:
        df[target_col] = df[target_col].fillna(group_stat)
        df[target_col] = df[target_col].fillna(df[target_col].agg(method))
    return df


def impute_zero_lines(ordersLines, txnsLines, columns=['Quantity', 'Rate', 'Total']):

    zero_mask = (ordersLines[columns] == 0).any(axis=1)
    ordersLines.loc[zero_mask, columns] = 0
    for col in columns:
        median_val = txnsLines[col].median()
        ordersLines.loc[ordersLines[col] == 0, col] = median_val

    if 'Quantity' in columns and 'Rate' in columns and 'Total' in columns:
        ordersLines['Total'] = ordersLines['Quantity'] * ordersLines['Rate']

    return ordersLines


def read_excel_from_googlesheets(apiKey, spreadsheetId, sheetName):
    try:
        sheet = build('sheets', 'v4', developerKey=apiKey).spreadsheets()
        sheet_data = sheet.values().get(spreadsheetId=spreadsheetId, range=f"{sheetName}").execute()
        sheet_values = sheet_data.get('values', [])
        if not sheet_values:
            raise ValueError("Google Sheet is empty or range is incorrect.")
        
        max_cols = max(len(row) for row in sheet_values)
        
        def col_letter(n):
            result = ''
            while n > 0:
                n, remainder = divmod(n-1, 26)
                result = chr(65 + remainder) + result
            return result
        last_col_letter = col_letter(max_cols)
        dynamic_range = f"{sheetName}!A:{last_col_letter}"
        sheet_data = sheet.values().get(spreadsheetId=spreadsheetId, range=dynamic_range).execute()
        sheet_values = sheet_data.get('values', [])
        headers = [h.strip() for h in sheet_values[0]]
        data_rows = [row + ['']*(len(headers)-len(row)) for row in sheet_values[1:]]
        df = pd.DataFrame(data_rows, columns=headers)
        return df
    except HttpError as e:
        raise Exception(f"Google Sheets API request failed: {e}")  
    except Exception as e:
        raise Exception(f"An error occurred while fetching data: {e}")


def get_access_token(client_id, client_secret, username, password, token_url):

    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "password",
        "username": username,
        "password": password
    }
    response = requests.post(token_url, headers=headers, data=payload)
    if response.ok:
        access_token = response.json().get("access_token")
        refresh_token = response.json().get("refresh_token")
        print("Access Token Retrieved!")
        return access_token, refresh_token
    else:
        print(f"[ERROR] Authorization Failed. Status Code: {response.status_code}")
        print(response.text)
        return None


def refresh_access_token(client_id, client_secret, refresh_token, token_url):

    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    response = requests.post(token_url, headers=headers, data=payload)
    if response.ok:
        access_token = response.json().get("access_token")
        refresh_token = response.json().get("refresh_token")
        return access_token, refresh_token
    else:
        print(f"[ERROR] Failed to Retrieve Refreshed Access Token! Authorization Failed. Status Code: {response.status_code}")
        print(response.text)
        return None

def get_resource(api_url, params=None):

    global client_id, client_secret, access_token, refresh_token, token_url
    access_token, refresh_token = refresh_access_token(client_id, client_secret, refresh_token, token_url)
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    if params:
        response = requests.get(api_url, headers=headers, params=params)
    else:
        response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"[ERROR] Failed to retrieve the resource!")
        print(json.loads(response.text))
        return response

def get_full_resource(api_url):

    resources = []
    params = {
    "$count": "true"
    }
    response = get_resource(api_url, params)
    total_number = response.get('@odata.count', None)
    table_name = response.get('@odata.context', None).split('#')[-1]
    if total_number is not None:
        total_pages = total_number// 50 + (total_number % 50 > 0)
        with tqdm(total=total_pages, desc=f'Fetching "{table_name}"') as pbar:
            while True:
                response = get_resource(api_url)
                resources.extend(response['value'])
                pbar.update(1)
                try:
                    api_url = response['@odata.nextLink']
                    time.sleep(1)
                except:
                    break

    else:
        page_index = 0
        while True:
            response = get_resource(api_url, params)
            resources.extend(response['value'])
            print(f"Page {page_index} added!")
            try:
                api_url = response['@odata.nextLink']
                page_index += 1
                time.sleep(1)
            except:
                print(f"All pages retrieved!")
                break

    df = pd.DataFrame(resources)
    return df, table_name

def list_timestream_tables(timestream_write_client, database_name):
    try:
        response = timestream_write_client.list_tables(DatabaseName=database_name)
        tables = [i.get('TableName', []) for i in response.get('Tables', [])]
        return tables
    except Exception as e:
        print(f"Error listing tables in {database_name}: {e}")
        return []

def fetch_data_from_timestream(timestream_query_client, query): 
    paginator = timestream_query_client.get_paginator('query')
    count_query = f"SELECT count(*) FROM ({query})"
    count_page_iterator = paginator.paginate(QueryString=count_query)
    total_rows = 0
    for count_page in count_page_iterator:
        if count_page['Rows']:
            total_rows = int(count_page['Rows'][0]['Data'][0]['ScalarValue'])
    if total_rows == 0:
        return pd.DataFrame()
    all_rows = []
    column_headers = []
    page_iterator = paginator.paginate(QueryString=query)
    first_page = True
    with tqdm(total=total_rows, desc="Fetching Data", unit="row") as pbar:
        for page in page_iterator:
            if first_page:
                column_headers = [col['Name'] for col in page['ColumnInfo']]
                first_page = False
            for row in page['Rows']:
                row_data = [value['ScalarValue'] if 'ScalarValue' in value else None for value in row['Data']]
                all_rows.append(row_data)
                pbar.update(1)
    df = pd.DataFrame(all_rows, columns=column_headers)
    return df


def upload_to_timestream(timestream_write_client, df, database_name, table_name):
    try:
        timestream_write_client.delete_table(DatabaseName=database_name, TableName=table_name)
        timestream_write_client.create_table(DatabaseName=database_name, TableName=table_name)
        prompt = f'{print_date_time()}\t\t[SUCCESS] Table "{table_name}" deleted & created!'
        print(prompt)
        write_file('log.txt' , f"{prompt}")

    except Exception as e:
        prompt = f'{print_date_time()}\t\t[ERROR] Error deleting or creating table: {e}'
        print(prompt)
        write_file('log.txt' , f"{prompt}")
        raise
    try:
        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc=f'Uploading "{table_name}" to TimeStream', unit="Record"):
            timestamp = int(datetime.now().timestamp() * 1000)
            record = {
                'Dimensions': [{'Name': dim, 'Value': str(row[dim])} for dim in df.columns.to_list()],
                'MeasureName': '_',
                'MeasureValue': '_',
                'MeasureValueType': 'VARCHAR',
                'Time': str(timestamp)
            }

            timestream_write_client.write_records(DatabaseName=database_name, TableName=table_name, Records=[record])
        prompt = f'{print_date_time()}\t\t[SUCCESS] Table "{table_name}" Loaded to Timestream "{database_name}" database!'
        print(prompt)
        write_file('log.txt' , f"{prompt}")
    except Exception as e:
        prompt = f'{print_date_time()}\t\t[ERROR] Failed to load "{table_name}" to Timestream. Error: {str(e)}'
        print(prompt)
        write_file('log.txt' , f"{prompt}")
        raise


def load_permissions_data(
    timestream_query_client, 
    timestream_write_client, 
    permissions_dataset, 
    processedAccess, 
    unProcessedAccess, 
    requiredMeasureNames, 
    database_name = None, 
    table_name = None
):
    query = """
            SELECT deviceId, measure_name, COUNT(*) AS "Number of observation"
            FROM "KomarEwonDB"."EwonDataTable"
            GROUP BY deviceId, measure_name
            """
    ts_df = fetch_data_from_timestream(timestream_query_client, query)
    def has_required_measures(group):
        return set(requiredMeasureNames).issubset(set(group['measure_name']))
    qualified_devices = ts_df.groupby('deviceId').filter(has_required_measures)
    qualified_devices = qualified_devices['deviceId'].unique()
    all_devices = sorted(ts_df['deviceId'].unique())
    authorized_devices_count = len(qualified_devices)
    all_devices_count = len(all_devices)

    unprocessed_users_list = "\n".join(f"\t- {user}" for user in unProcessedAccess)
    processed_users_list = "\n".join(f"\t- {user}" for user in processedAccess)
    prompt = f"""
    User Access Details:
    All devices will be available to:
    {unprocessed_users_list}
    {authorized_devices_count} out of {all_devices_count} devices are qualified and will be available to:
    {processed_users_list}
    """
    print(prompt)
    write_file('log.txt' , f"{prompt}")

    default_permissions = []
    for user in unProcessedAccess:
        for device in all_devices:
            default_permissions.append({'UserName': user, 'deviceId': device})
    for user in processedAccess:
        for device in qualified_devices:
            default_permissions.append({'UserName': user, 'deviceId': device})
    default_permissions_df = pd.DataFrame(default_permissions)
    updated_permissions_dataset = pd.concat([permissions_dataset, default_permissions_df], ignore_index=True)

    if database_name and table_name:
        upload_to_timestream(timestream_write_client, updated_permissions_dataset[['UserName', 'deviceId']], database_name, table_name)
    return qualified_devices


def t2m_login(base_url, developer_id, account, username, password):
    try:
        response = requests.get(f"{base_url}login?t2maccount={account}&t2musername={username}&t2mpassword={password}&t2mdeveloperid={developer_id}")
        response_data = response.json()
        if response_data.get('success') == True:
            pass # print(f"[SUCCESS] Logged into Talk2M!")
            return response_data['t2msession']
        else:
            raise Exception()
    except:    
        print(f"[ERROR] t2m_login Failed: {response.text}")


def t2m_logout(base_url, session_id, developer_id):
    try:
        response = requests.get(f"{base_url}logout?t2msession={session_id}&t2mdeveloperid={developer_id}")        
        response_data = response.json()
        if response_data.get('success') == True:
            pass # print(f"[SUCCESS] Logged out of Talk2M!")
        else:
            raise Exception()
    except:    
        print(f"[ERROR] t2m_logout Failed: {response.text}")


def get_account_info(base_url, developer_id, session_id=None):
    try:
        temporary_session = False
        if session_id is None:
            session_id = t2m_login(base_url, developer_id, account, username, password)
            temporary_session = True
        response = requests.get(f"{base_url}getaccountinfo?t2msession={session_id}&t2mdeveloperid={developer_id}") 
        if temporary_session:
            t2m_logout(base_url, session_id, developer_id)
        response_data = response.json()
        if response_data.get('success') == True:
            print(f"[SUCCESS] Retrieved account information!")
            return response_data
        else:
            raise Exception()
    except:    
        print(f"[ERROR] Failed to Retrieve account information: {response.text}")


def get_ewons(base_url, developer_id, session_id=None):
    try:
        temporary_session = False
        if session_id is None:
            session_id = t2m_login(base_url, developer_id, account, username, password)
            temporary_session = True
        response = requests.get(f"{base_url}getewons?t2msession={session_id}&t2mdeveloperid={developer_id}")
        if temporary_session:
            t2m_logout(base_url, session_id, developer_id)
        response_data = response.json()
        if response_data.get('success') == True:
            print(f"[SUCCESS] Retrieved Ewons!")
            df = pd.DataFrame(response_data.get('ewons'))
            return df
        else:
            raise Exception()
    except:    
        print(f"[ERROR] Failed to Retrieve Ewons: {response.text}")


def get_ewon(base_url, developer_id, ewon_id, session_id=None):
    temporary_session = False
    if session_id is None:
        session_id = t2m_login(base_url, developer_id, account, username, password)
        temporary_session = True
    response = requests.get(f"{base_url}getewon?id={ewon_id}&t2msession={session_id}&t2mdeveloperid={developer_id}")
    if temporary_session:
        t2m_logout(base_url, session_id, developer_id)
    return response.json()


def get_ewon_details(base_url, developer_id, encodedName, device_username, device_password, account, username, password, session_id=None):

    try:
        temporary_session = False
        if session_id is None:
            session_id = t2m_login(base_url, developer_id, account, username, password)
            temporary_session = True
        response = requests.get(f"{base_url}get/{encodedName}/rcgi.bin/ParamForm?AST_Param=$dtES$ftH$fn&t2msession={session_id}&t2mdeveloperid={developer_id}&t2mdeviceusername={device_username}&t2mdevicepassword={device_password}")
        if temporary_session:
            t2m_logout(base_url, session_id, developer_id)
        if response.status_code == 200:
            response_text = response.text
            try:
                soup = BeautifulSoup(response_text, 'html.parser')
                rows = soup.find_all('tr')
                parsed_data = {}
                for row in rows:
                    try:
                        cell = row.find('td')
                        if cell and ':' in cell.text:
                            key, value = cell.text.strip().split(':', 1)
                            parsed_data[key.strip()] = value.strip()
                        else:
                            print(f"[WARNING] Skipping row with unexpected format: {row}")
                    except Exception as e:
                        print(f"[ERROR] Failed to process row '{row}': {e}")
                # print(f"[SUCCESS] Retrieved Ewon Details!")
                return parsed_data

            except Exception as e:
                print(f"[ERROR] Failed to parse HTML: {e}")
                return {}
            # soup = BeautifulSoup(response_text, 'html.parser')
            # table = soup.find('table', {'class': 'edbt'})
            # parsed_data = {}
            # for row in table.find_all('tr'):
            #     cells = row.find_all('td')
            #     if len(cells) == 1:
            #         key_value = cells[0].text.split(':')
            #         if len(key_value) == 2:
            #             parsed_data[key_value[0]] = key_value[1]
            # print(f"Get Ewon Details Successful!")
            # return parsed_data
        else:
            raise Exception()
    except:    
        # print(f"Get Ewon Details Failed: {response.text}")
        return {}


def fetch_iot_things(iot_client):
    things_df_list = []
    response = iot_client.list_things(maxResults=250)
    next_token = response.get('nextToken', None)
    things_df_list.append(pd.DataFrame(response['things']))
    while next_token:
        response = iot_client.list_things(maxResults=250, nextToken=next_token)
        things_df_list.append(pd.DataFrame(response['things']))
        next_token = response.get('nextToken', None)
    things = pd.concat(things_df_list, axis=0)    
    return things


def delete_thing_and_certificates(iot_client, thing_name):
    try:
        iot_client.describe_thing(thingName=thing_name)
    except iot_client.exceptions.ResourceNotFoundException:
        print(f"[WARNING] Thing '{thing_name}' does not exist.")
        return
    try:
        principals = iot_client.list_thing_principals(thingName=thing_name)['principals']
        for principal in principals:
            print(f"[INFO] Detaching certificate: {principal}")
            iot_client.detach_thing_principal(
                thingName=thing_name,
                principal=principal
            )
            policies = iot_client.list_attached_policies(target=principal)['policies']
            for policy in policies:
                print(f"[INFO] Detaching policy '{policy['policyName']}' from certificate...")
                iot_client.detach_policy(
                    policyName=policy['policyName'],
                    target=principal
                )
            cert_id = principal.split('/')[-1]
            print(f"[INFO] Deactivating certificate: {cert_id}")
            iot_client.update_certificate(
                certificateId=cert_id,
                newStatus='INACTIVE'
            )
            print(f"[INFO] Deleting certificate: {cert_id}")
            iot_client.delete_certificate(
                certificateId=cert_id,
                forceDelete=True
            )
    except botocore.exceptions.ClientError as e:
        print(f"[ERROR] Failed to clean up certificates or policies for '{thing_name}': {e}")
        return

    try:
        iot_client.delete_thing(thingName=thing_name)
        print(f"[SUCCESS] Deleted Thing '{thing_name}' and all associated certificates!\n")
    except botocore.exceptions.ClientError as e:
        print(f"[ERROR] Failed to delete Thing '{thing_name}': {e}")


def restart_device_via_web_ui(ip_address, username, password, wait_time=30):
    print("[INFO] Restarting...")
    try:
        options = webdriver.ChromeOptions()
        # options.add_argument("--headless")
        options.add_argument("--disable-extensions")
        # options.add_argument("--disable-logging")
        options.add_argument("--log-level=3")
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        driver.get(f'http://{ip_address}')
        wait = WebDriverWait(driver, wait_time)

        def find_and_act(element_id, action='click', text=None, max_attempts=10):
            attempts = 0
            while attempts < max_attempts:
                try:
                    element = wait.until(EC.presence_of_element_located((By.ID, element_id)))
                    driver.execute_script("arguments[0].scrollIntoView(true);", element)
                    time.sleep(0.5)
                    if action == 'click':
                        element.click()
                    elif action == 'send_keys' and text is not None:
                        element.send_keys(text)
                    elif action == 'enter':
                        element.send_keys(Keys.ENTER)
                    return True

                except Exception as e:
                    print(f"[INFO] Attempt {attempts+1}/{max_attempts}: Element '{element_id}' not interactable... Retrying...")
                    attempts += 1
                    time.sleep(1)
            print(f"[ERROR] Failed to interact with element '{element_id}' after {max_attempts} attempts.")
            print("[ERROR] Failed to reboot the device.")
            return False

        if not find_and_act('textfield-1056-inputEl', action='send_keys', text=username): return False
        if not find_and_act('textfield-1057-inputEl', action='send_keys', text=password): return False
        if not find_and_act('button-1061-btnInnerEl'): return False
        if not find_and_act('ext-element-378'): return False
        if not find_and_act('ext-element-375'): return False
        if not find_and_act('btn_Reboot-btnInnerEl'): return False

        try:
            reboot_message = wait.until(EC.presence_of_element_located((By.XPATH, "//span[contains(text(), 'Reboot will occur...')]")))
            if reboot_message:
                print("[INFO] Reboot message received.")
                print("[SUCCESS] Device will reboot shortly!")
                return True
        except Exception as ValueError:
            print(f"[ERROR] Reboot message not found: {ValueError}.")
            print("[ERROR] Failed to reboot the device.")
            return False
    except Exception as e:
        print(f"[ERROR] An error occurred during device restart: {e}")
        print("[ERROR] Failed to reboot the device.")
        return False
    finally:
        if driver:
            driver.quit()


def cleanup_device_driver_files(ip_address, username, password):
    try:
        print(f"[INFO] Cleaning up the driver...")
        with ftplib.FTP(ip_address) as ftp:
            ftp.login(user=username, passwd=password)
            # print("[SUCCESS] Logged into device!")
            directories = ftp.nlst()
            if 'usr' not in directories:
                print("[ERROR] The 'usr' directory is missing on the device!")
                return False
            ftp.cwd('/usr')
            usr_files = ftp.nlst()
            files_to_delete = [
                f for f in usr_files if (
                    'flexy-aws-connector' in f or
                    'jvmrun' in f or
                    'AwsConnectorConfig.json' in f
                )
            ]
            folders_to_delete = [
                f for f in usr_files if (
                    'AwsCertificates' in f or
                    'hist-data-queue' in f
                )
            ]
            if not files_to_delete and not folders_to_delete:
                print("[INFO] No cleanup needed — already clean.")
                return True
            for file in files_to_delete:
                try:
                    ftp.delete(file)
                    print(f"[INFO] Deleted file: {file}")
                except ftplib.all_errors as e:
                    print(f"[ERROR] Failed to delete file {file}: {e}")
            def delete_directory_and_contents(ftp, dir_name):
                try:
                    ftp.cwd(dir_name)
                    files = ftp.nlst()
                    for f in files:
                        try:
                            ftp.delete(f)
                            print(f"[INFO] Deleted file '{f}' inside '{dir_name}'")
                        except Exception as e:
                            print(f"[ERROR] Could not delete '{f}' in '{dir_name}': {e}")
                    ftp.cwd("..")
                    ftp.rmd(dir_name)
                    print(f"[INFO] Deleted directory: {dir_name}")
                except Exception as e:
                    print(f"[ERROR] Failed to delete directory '{dir_name}': {e}")
            for folder in folders_to_delete:
                delete_directory_and_contents(ftp, folder)
            print("[SUCCESS] Cleanup complete!")
            return True
    except ftplib.all_errors as e:
        print(f"[ERROR] FTP connection or operation failed: {e}")
        return False


def install_device_driver_files(ip_address, username, password, latest_driver_jar, files_to_upload_to_usr, files_to_upload_to_AwsCertificates, source_type, s3_bucket_name = None, s3_client=None):
    try:
        with ftplib.FTP(ip_address) as ftp:
            ftp.login(user=username, passwd=password)
            # print("[SUCCESS] Logged into device!")
            directories = ftp.nlst()
            if 'usr' not in directories:
                print("[ERROR] The 'usr' directory is missing on the device!")
                return False
            ftp.cwd('/usr')
            usr_files = ftp.nlst()
            files_to_delete = [
                f for f in usr_files if (
                    'flexy-aws-connector' in f or
                    'jvmrun' in f or
                    'AwsConnectorConfig.json' in f
                )
            ]
            folders_to_delete = [
                f for f in usr_files if (
                    'AwsCertificates' in f or
                    'hist-data-queue' in f
                )
            ]
            if latest_driver_jar in usr_files:
                print("[INFO] Driver is already installed!")
                return "Already Installed"
            elif not files_to_delete and not folders_to_delete:
                print("[INFO] Installing the driver...")
                for file_name, file_path in files_to_upload_to_usr.items():
                    if source_type == 'local':
                        try:
                            with open(file_path, 'rb') as fp:
                                ftp.storbinary(f'STOR {file_name}', fp)
                        except FileNotFoundError:
                            print(f"[ERROR]  File not found: {file_path}")
                            return False
                        except Exception as e:
                            print(f"[ERROR]  Failed to upload {file_name}: {e}")
                            return False
                    elif source_type == 's3':
                        s3_object = s3_client.get_object(Bucket=s3_bucket_name, Key=file_path)
                        try:
                            with s3_object['Body'] as fp:
                                ftp.storbinary(f'STOR {file_name}', fp)
                        except Exception as e:
                            print(f"[ERROR] Failed to upload {file_name} from S3: {e}")
                            return False
                    else:
                        print(f"[ERROR] Unknown source type: {source_type}")
                        return False

                ftp.mkd('AwsCertificates')
                ftp.cwd('AwsCertificates')
                for file_name, file_path in files_to_upload_to_AwsCertificates.items():
                    if source_type == 'local':
                        try:
                            with open(file_path, 'rb') as fp:
                                ftp.storbinary(f'STOR {file_name}', fp)
                        except FileNotFoundError:
                            print(f"[ERROR]  File not found: {file_path}")
                            return False
                        except Exception as e:
                            print(f"[ERROR] Failed to upload {file_name}: {e}")
                            return False
                    elif source_type == 's3':
                        s3_object = s3_client.get_object(Bucket=s3_bucket_name, Key=file_path)
                        try:
                            with s3_object['Body'] as fp:
                                ftp.storbinary(f'STOR {file_name}', fp)
                        except Exception as e:
                            print(f"[ERROR] Failed to upload {file_name} from S3: {e}")
                            return False
                    else:
                        print(f"[ERROR] Unknown source type: {source_type}")
                        return False
                print("[SUCCESS] Install complete!")
                return True
            else:
                return "Cleanup Needed"

    except ftplib.all_errors as e:
        print(f"[ERROR] FTP connection or operation failed: {e}")
        return False


def stop_driver(ip_address, username, password):

    url = f"http://{ip_address}//rcgi.bin/jvmCmd?cmd=stop"  

    try:
        response = requests.get(url, auth=HTTPBasicAuth(username, password))
        print()
        print()

        if response.status_code == 200 and response.text.strip() == "JVM Stopped":
            print("[SUCCESS] JVM stop command completed!")
            return True
        else:
            print("[WARNING] Unexpected response received:")
            print(f"  - Status Code: {response.status_code}")
            print(f"  - Response: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed: {e}")
        return False


def install_device_firmware(
        ip_address, 
        username, 
        password, 
        firmware_file_name,
        firmware_file_path, 
        s3_bucket_name, 
        s3_client,
        max_retries=3,
):
    print(f"[INFO] Installing the firmware...")

    for attempt in range(1, max_retries + 1):
        try:
            with ftplib.FTP(ip_address, timeout=120) as ftp:
                # ftp.set_debuglevel(2)
                ftp.login(user=username, passwd=password)
                try:
                    ftp.set_pasv(True)
                except Exception as e:
                    print(f"[INFO] Passive mode failed: {e}. Switching to active mode...")
                    ftp.set_pasv(False)
                ftp.sock.settimeout(120)
                ftp.sendcmd('TYPE I')
                ftp.cwd('/')
                # print("[SUCCESS] Logged into device!")
                s3_object = s3_client.get_object(Bucket=s3_bucket_name, Key=firmware_file_path)
                firmware_data = s3_object['Body'].read()
                firmware_size = len(firmware_data)
                fp = io.BytesIO(firmware_data)
                fp.seek(0)
                with tqdm.wrapattr(fp, "read", total=firmware_size, desc="Installing firmware", unit="B", unit_scale=True) as wrapped_fp:
                    ftp.storbinary(f'STOR {firmware_file_name}', wrapped_fp, blocksize=32768)
                print("[SUCCESS] Firmware install complete!")
                return True

        except Exception as e:
            print(f'[ERROR] Failed to upload {firmware_file_name} from S3: {str(e)}. Retry {attempt + 1}/{max_retries} in 5 seconds...')
            time.sleep(5)

    print("[ERROR] Failed to install the firmware!")
    return False


def find_four_digit_number(string):
    match = re.search(r'\d{4}', string)
    if match:
        return match.group()
    else:
        return "No four-digit number found"


def timer_and_alert(seconds, sound_file):
    try:
        if seconds <= 0:
            winsound.PlaySound(sound_file, winsound.SND_FILENAME)
        else:
            for _ in tqdm(range(seconds), desc="[PENDING] Timer", unit="s"):
                time.sleep(1)
            winsound.PlaySound(sound_file, winsound.SND_FILENAME)
    except Exception as e:
        print(f"[ERROR] Failed to play sound: {e}")


def enrich_and_classify_customers(
        df,
        companyName,
        s3_client,
        s3_bucket_name,
        DBIA=False,
        dfCategories=None,
        txnsLines=None,
        dropLookUpIn = True,
        dfLevels =['CustomerLevel1', 'CustomerLevel2', 'CustomerLevel3', 'CustomerLevel4', 'CustomerLevel5'],
        lookUpCols = ['CustName'],
        key_cols = ['CustId', 'CustName'],
        df_cols = ['CustNo'],
        extraLevels = ['ParentName'],
        zip_code_columns = ['CustZip'],
        state_columns = ['CustState'],
        cat_object_key = 'customersCategories.csv',
        dst_object_key = 'customer.csv',
        idCol = 'CustId',
        idName = 'CustName',
        match_method = 'startswith',
        fillValues = 'CUSTOMER',
        ):

    erpId = 'ERP' + idCol
    searchCol = idCol + '_SearchKey'
    if DBIA:
        level_cols = dfLevels + ['CommonName'] + extraLevels
        for col in key_cols:
            txnsLines[col] = txnsLines[col].fillna(fillValues).astype('str').str.upper().str.strip()
        dfCategories_pred = txnsLines[key_cols].merge(dfCategories[key_cols], on = key_cols, how='left', indicator = True).drop_duplicates(subset = key_cols)
        dfCategories_pred = dfCategories_pred.query("_merge == 'left_only'").copy()
        if not dfCategories_pred.empty and not dfCategories.empty:
            sample_size = min(len(dfCategories[key_cols+level_cols].dropna()), 10000)
            labeled_df = dfCategories[key_cols+level_cols].dropna().sample(sample_size)
            delimiter = " :|: "
            labeled_df['target_col'] = labeled_df[level_cols].fillna('').agg(delimiter.join, axis=1)
            dfCategories_pred = train_and_predict(
                labeled_df = labeled_df,
                unlabeled_df = dfCategories_pred,
                input_cols = key_cols,
                target_cols = ['target_col']
            )
            split_cols = dfCategories_pred['target_col'].str.split(delimiter, expand=True, regex=False)
            split_cols.columns = level_cols
            dfCategories_pred = pd.concat([dfCategories_pred, split_cols], axis=1)
            dfCategories_pred.drop(columns = 'target_col', inplace=True)
            dfCategories_pred.reset_index(drop=True, inplace=True)
            dfCategories_pred.index = dfCategories_pred.index + 1 + dfCategories['index'].astype('int').max()
            dfCategories_pred.reset_index(inplace=True)
            dfCategories_pred['index'] = dfCategories_pred['index'].astype('int').astype('str')
            dfCategories = pd.concat([dfCategories, dfCategories_pred], ignore_index=True)
        dfNew = df.copy()
        dfNew_pred = dfCategories_pred.rename(columns = {idCol: erpId}).astype({erpId:'str'}).merge(dfNew[[erpId]+ df_cols].drop_duplicates(subset = [erpId]).astype({erpId:'str'}), on =erpId, how ='left').rename(columns = {'index': idCol}).copy()
        dfNew_pred['Company'] = companyName
        dfNew_pred = dfNew_pred[['Company'] + dfNew_pred.columns[:-1].tolist()]
        dfNew = pd.concat([dfNew, dfNew_pred], ignore_index=True)
        for col in df_cols:
            mask = dfNew[col].isna()
            dfNew.loc[mask, col] = dfNew.loc[mask, erpId]
        upload_to_s3(s3_client = s3_client, data = dfNew, bucket_name = s3_bucket_name + '-c', object_key = dst_object_key)
        txnsLines = txnsLines.merge(dfCategories[key_cols + ['index', 'CommonName']].drop_duplicates(subset = key_cols), on = key_cols, how='left').rename(columns = {idCol: erpId, 'index': idCol}).copy()
        txnsLines.drop(columns = [erpId], inplace = True)
        return txnsLines, dfCategories, dfNew
    else:
        df = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = df, df_name = 'df', id_column = [idCol], additional_date_columns = [], zip_code_columns = zip_code_columns, state_columns = state_columns, keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        for col in lookUpCols:
            df[col] = df[col].astype('str').fillna('')
        dfCategories = read_csv_from_s3(s3_client = s3_client, bucket_name = 'manual-db', object_key = cat_object_key)
        dfCategories_2 = dfCategories.loc[ (dfCategories['Company'] != companyName) & (dfCategories['Company'].notna()) ]
        dfCategories = dfCategories.loc[ (dfCategories['Company'] == companyName) | (dfCategories['Company'].isna()) ]
        dfCategories['Found'] = False
        dfCategories = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = dfCategories, df_name = 'dfCategories', id_column = [searchCol], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False)
        dfCategories = dfCategories.sort_values(by=searchCol, key=lambda x: x.str.len(), ascending=False)
        dfCategories = dfCategories[dfCategories[searchCol].notna()]
        df[idCol] = df[idCol].astype('str').str.upper()
        dfCategories[searchCol] = dfCategories[searchCol].astype('str').str.upper()
        df = df.merge(dfCategories[[searchCol, 'CommonName'] + extraLevels + dfLevels], left_on = idCol, right_on = searchCol, how = 'left')
        dfCategories.loc[dfCategories[searchCol].isin(df[searchCol]), 'Found'] = True
        dfCategories.loc[dfCategories[searchCol].isin(df[searchCol]), 'Company'] = companyName
        df.drop(columns = searchCol, inplace=True)
        df['LookUpIn'] = df[lookUpCols].fillna('').agg(' '.join, axis=1)
        df['LookUpIn'] = df['LookUpIn'].str.upper()
        keyword_dict = dfCategories.loc[~dfCategories['Found']==True].set_index(searchCol)['CommonName'].to_dict()
        for keyword, CommonName in keyword_dict.items():
            if match_method == 'startswith':
                match_condition = df['LookUpIn'].str.startswith(keyword)
            else:
                match_condition = df['LookUpIn'].str.contains(keyword, regex=False)
            found_mask = (
                (df.CommonName.isna()) & 
                (match_condition)
            )
            df.loc[found_mask, 'CommonName'] = CommonName
            if found_mask.any():
                dfCategories.loc[dfCategories[searchCol] == keyword, 'Found'] = True
                dfCategories.loc[(dfCategories['Company'].isna())&(dfCategories[searchCol] == keyword), 'Company'] = companyName
        dfCategories_combined = pd.concat([dfCategories, dfCategories_2], ignore_index=True)
        upload_to_s3(s3_client = s3_client, data = dfCategories_combined, bucket_name = 'manual-db', object_key = cat_object_key)
        print(dfCategories.Found.sum())
        dfCategories = dfCategories[~dfCategories['CommonName'].duplicated()]
        df_2 = df.loc[~df[extraLevels + dfLevels].isna().all(axis=1)].copy()
        df = df.loc[df[extraLevels + dfLevels].isna().all(axis=1)].copy()
        df.drop(columns = extraLevels + dfLevels, inplace=True)
        df = df.merge(dfCategories[['CommonName'] + extraLevels + dfLevels], on='CommonName', how = 'left')
        df = pd.concat([df, df_2], ignore_index=True)
        for col in ['CommonName'] + extraLevels:
            df.loc[df[col].isna(), col] = df[idName].str[0:15]
        for dfLevel in dfLevels:
            df.loc[df[dfLevel].isna(), dfLevel] = 'OTHER'
        missingRow = {}
        for col in [idCol] + lookUpCols:
            missingRow[col]=['Missing']
        for col in dfLevels + ['CommonName']:
            missingRow[col]=['OTHER']
        missingRow = pd.DataFrame(missingRow)
        df = pd.concat([df, missingRow], ignore_index=True)
        df['Company'] = companyName
        df = df[['Company'] + df.columns[:-1].tolist()].copy()
        if dropLookUpIn:
            df.drop(columns = 'LookUpIn', inplace=True)
        upload_to_s3(s3_client = s3_client, data = df, bucket_name = s3_bucket_name + '-c', object_key = dst_object_key)
        prompt = f'Found: {dfCategories.Found.sum()}...'
        print(prompt)
        write_file('log.txt' , f"{print_date_time()}\t\t{prompt}")
        return df
    

def enrich_and_classify_items(
        df,
        companyName,
        s3_client,
        s3_bucket_name,
        DBIA=False,
        dfCategories=None,
        txnsLines=None,
        dropLookUpIn = False,
        dfLevels =['ItemLevel1', 'ItemLevel2', 'ItemLevel3', 'ItemLevel4', 'ItemLevel5'],
        lookUpCols = ['ItemNo', 'ItemName', 'ItemDescription'],
        key_cols = ['ItemId', 'ItemDescription'],
        df_cols = ['ItemNo', 'ItemName'],
        extraLevels = [],
        zip_code_columns = [],
        state_columns = [],
        cat_object_key = 'itemsCategories.csv',
        dst_object_key = 'item.csv',
        idCol = 'ItemId',
        idName = 'ItemName',
        match_method = 'contains',
        fillValues = 'ITEM'
        ):

    erpId = 'ERP' + idCol
    searchCol = idCol + '_SearchKey'
    if DBIA:
        level_cols = dfLevels + ['CommonName'] + extraLevels
        for col in key_cols:
            txnsLines[col] = txnsLines[col].fillna(fillValues).astype('str').str.upper().str.strip()
        dfCategories_pred = txnsLines[key_cols].merge(dfCategories[key_cols], on = key_cols, how='left', indicator = True).drop_duplicates(subset = key_cols)
        dfCategories_pred = dfCategories_pred.query("_merge == 'left_only'").copy()
        if not dfCategories_pred.empty and not dfCategories.empty:
            sample_size = min(len(dfCategories[key_cols+level_cols].dropna()), 10000)
            labeled_df = dfCategories[key_cols+level_cols].dropna().sample(sample_size)
            delimiter = " :|: "
            labeled_df['target_col'] = labeled_df[level_cols].fillna('').agg(delimiter.join, axis=1)
            dfCategories_pred = train_and_predict(
                labeled_df = labeled_df,
                unlabeled_df = dfCategories_pred,
                input_cols = key_cols,
                target_cols = ['target_col']
            )
            split_cols = dfCategories_pred['target_col'].str.split(delimiter, expand=True, regex=False)
            split_cols.columns = level_cols
            dfCategories_pred = pd.concat([dfCategories_pred, split_cols], axis=1)
            dfCategories_pred.drop(columns = 'target_col', inplace=True)
            dfCategories_pred.reset_index(drop=True, inplace=True)
            dfCategories_pred.index = dfCategories_pred.index + 1 + dfCategories['index'].astype('int').max()
            dfCategories_pred.reset_index(inplace=True)
            dfCategories_pred['index'] = dfCategories_pred['index'].astype('int').astype('str')
            dfCategories = pd.concat([dfCategories, dfCategories_pred], ignore_index=True)
        dfNew = df.copy()
        dfNew_pred = dfCategories_pred.rename(columns = {idCol: erpId}).astype({erpId:'str'}).merge(dfNew[[erpId]+ df_cols].drop_duplicates(subset = [erpId]).astype({erpId:'str'}), on =erpId, how ='left').rename(columns = {'index': idCol}).copy()
        dfNew_pred['Company'] = companyName
        dfNew_pred = dfNew_pred[['Company'] + dfNew_pred.columns[:-1].tolist()]
        dfNew = pd.concat([dfNew, dfNew_pred], ignore_index=True)
        for col in df_cols:
            mask = dfNew[col].isna()
            dfNew.loc[mask, col] = dfNew.loc[mask, erpId]
        upload_to_s3(s3_client = s3_client, data = dfNew, bucket_name = s3_bucket_name + '-c', object_key = dst_object_key)
        txnsLines = txnsLines.merge(dfCategories[key_cols + ['index', 'CommonName']].drop_duplicates(subset = key_cols), on = key_cols, how='left').rename(columns = {idCol: erpId, 'index': idCol}).copy()
        txnsLines.drop(columns = [erpId], inplace = True)
        return txnsLines, dfCategories, dfNew
    else:
        df = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = df, df_name = 'df', id_column = [idCol], additional_date_columns = [], zip_code_columns = zip_code_columns, state_columns = state_columns, keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False )
        for col in lookUpCols:
            df[col] = df[col].astype('str').fillna('')
        dfCategories = read_csv_from_s3(s3_client = s3_client, bucket_name = 'manual-db', object_key = cat_object_key)
        dfCategories_2 = dfCategories.loc[ (dfCategories['Company'] != companyName) & (dfCategories['Company'].notna()) ]
        dfCategories = dfCategories.loc[ (dfCategories['Company'] == companyName) | (dfCategories['Company'].isna()) ]
        dfCategories['Found'] = False
        dfCategories = clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name, df = dfCategories, df_name = 'dfCategories', id_column = [searchCol], additional_date_columns = [], zip_code_columns = [], keep_invalid_as_null=True, numeric_id=False, just_useful_columns=False)
        dfCategories = dfCategories.sort_values(by=searchCol, key=lambda x: x.str.len(), ascending=False)
        dfCategories = dfCategories[dfCategories[searchCol].notna()]
        df[idCol] = df[idCol].astype('str').str.upper()
        dfCategories[searchCol] = dfCategories[searchCol].astype('str').str.upper()
        df = df.merge(dfCategories[[searchCol, 'CommonName'] + extraLevels + dfLevels], left_on = idCol, right_on = searchCol, how = 'left')
        dfCategories.loc[dfCategories[searchCol].isin(df[searchCol]), 'Found'] = True
        dfCategories.loc[dfCategories[searchCol].isin(df[searchCol]), 'Company'] = companyName
        df.drop(columns = searchCol, inplace=True)
        df['LookUpIn'] = df[lookUpCols].fillna('').agg(' '.join, axis=1)
        df['LookUpIn'] = df['LookUpIn'].str.upper()
        keyword_dict = dfCategories.loc[~dfCategories['Found']==True].set_index(searchCol)['CommonName'].to_dict()
        for keyword, CommonName in keyword_dict.items():
            if match_method == 'startswith':
                match_condition = df['LookUpIn'].str.startswith(keyword)
            else:
                match_condition = df['LookUpIn'].str.contains(keyword, regex=False)
            found_mask = (
                (df.CommonName.isna()) & 
                (match_condition)
            )
            df.loc[found_mask, 'CommonName'] = CommonName
            if found_mask.any():
                dfCategories.loc[dfCategories[searchCol] == keyword, 'Found'] = True
                dfCategories.loc[(dfCategories['Company'].isna())&(dfCategories[searchCol] == keyword), 'Company'] = companyName
        dfCategories_combined = pd.concat([dfCategories, dfCategories_2], ignore_index=True)
        upload_to_s3(s3_client = s3_client, data = dfCategories_combined, bucket_name = 'manual-db', object_key = cat_object_key)
        print(dfCategories.Found.sum())
        dfCategories = dfCategories[~dfCategories['CommonName'].duplicated()]
        df_2 = df.loc[~df[extraLevels + dfLevels].isna().all(axis=1)].copy()
        df = df.loc[df[extraLevels + dfLevels].isna().all(axis=1)].copy()
        df.drop(columns = extraLevels + dfLevels, inplace=True)
        df = df.merge(dfCategories[['CommonName'] + extraLevels + dfLevels], on='CommonName', how = 'left')
        df = pd.concat([df, df_2], ignore_index=True)
        for col in ['CommonName'] + extraLevels:
            df.loc[df[col].isna(), col] = df[idName].str[0:15]
        for dfLevel in dfLevels:
            df.loc[df[dfLevel].isna(), dfLevel] = 'OTHER'
        missingRow = {}
        for col in [idCol] + lookUpCols:
            missingRow[col]=['Missing']
        for col in dfLevels + ['CommonName']:
            missingRow[col]=['OTHER']
        missingRow = pd.DataFrame(missingRow)
        df = pd.concat([df, missingRow], ignore_index=True)
        df['Company'] = companyName
        df = df[['Company'] + df.columns[:-1].tolist()].copy()
        if dropLookUpIn:
            df.drop(columns = 'LookUpIn', inplace=True)
        upload_to_s3(s3_client = s3_client, data = df, bucket_name = s3_bucket_name + '-c', object_key = dst_object_key)
        prompt = f'Found: {dfCategories.Found.sum()}...'
        print(prompt)
        write_file('log.txt' , f"{print_date_time()}\t\t{prompt}")
        return df
    
    
def classify_items_rrs(
    transactions,
    transactionsLines,
    items,
    runner_threshold = 0.4,
    repeater_threshold = 0.1
):

    df = transactions[['TransactionId', 'TransactionDate']].drop_duplicates(subset='TransactionId').merge(
        transactionsLines[['TransactionId', 'ItemId']], 
        on='TransactionId'
    ).merge(
        items[['ItemId', 'CommonName']].drop_duplicates(subset='ItemId'), 
        on='ItemId'
    )
    df = df[df['TransactionDate'] >= datetime.now() - timedelta(days=365)]
    df = df.groupby('CommonName').agg({'TransactionDate': 'nunique'}).reset_index()
    df.rename(columns={'TransactionDate': 'DaysSold'}, inplace=True)
    max_days = df['DaysSold'].max()
    runner_threshold = runner_threshold * max_days
    repeater_threshold = repeater_threshold * max_days
    
    def classify_item(days_sold):
        if days_sold >= runner_threshold:
            return 'Runner'
        elif days_sold >= repeater_threshold:
            return 'Repeater'
        else:
            return 'Stranger'
    
    df['RRS'] = df['DaysSold'].apply(classify_item)
    df.sort_values('DaysSold', ascending=False)
    items = items.merge(df[['CommonName', 'RRS']].drop_duplicates(subset = 'CommonName'), on='CommonName', how = 'left')
    return items


def read_excel_from_sharepoint(url):
    response = requests.get(url)
    if response.status_code == 200:
        match = re.search(r'var _wopiContextJson\s*=\s*(\{.*?\});', response.text, re.DOTALL)
        if match:
            wopi_context = json.loads(match.group(1))
            file_get_url = wopi_context.get("FileGetUrl")
            if file_get_url:
                file_response = requests.get(file_get_url, stream=True)
                file_size = int(file_response.headers.get('content-length', 0))
                progress = tqdm(total=file_size, unit='B', unit_scale=True, desc='Downloading Excel file')
                xlsx_data = io.BytesIO()
                for chunk in file_response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        xlsx_data.write(chunk)
                        progress.update(len(chunk))
                progress.close()
                xlsx_data.seek(0)
                df = pd.read_excel(xlsx_data, engine='openpyxl')
                return df
            else:
                raise ValueError("FileGetUrl not found in WOPI context.")
        else:
            raise ValueError("WOPI context JSON not found in the response.")
    else:
        raise Exception(f"[ERROR] Failed to fetch the URL. Status code: {response.status_code}")


def generate_table_select_queries(
    project_id,
    bigquery_client,
    tables_to_remove=None
):
    table_queries = {}
    datasets = list(bigquery_client.list_datasets())
    if datasets:
        for dataset in datasets:
            table_list = bigquery_client.list_tables(dataset.dataset_id)
            for table in table_list:
                table_queries[table.table_id] = f"SELECT * FROM `{table.project}.{table.dataset_id}.{table.table_id}`"
    else:
        print(f"{project} project does not contain any datasets.")
    if tables_to_remove:
        for table in tables_to_remove:
            if table in table_queries:
                del table_queries[table]
    return table_queries

def load_suiteql_data_via_query(
    consumer_key, 
    consumer_secret, 
    token_key, 
    token_secret, 
    realm, 
    query, 
    limit=1000
):
    auth = OAuth1(
        consumer_key,
        consumer_secret,
        token_key,
        token_secret,
        realm=realm,
        signature_method='HMAC-SHA256'
    )
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Prefer': 'transient'
    }
    offset = 0
    hasMore = True
    all_items = []
    total_results =0
    with tqdm(total=total_results, desc="Fetching data from NetSuite", unit="records") as pbar:
        while hasMore:
            suiteql_url = f'https://{realm}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql?limit={limit}&offset={offset}'
            response = requests.post(suiteql_url, auth=auth, headers=headers, json={"q": query})
            if response.status_code == 200:
                result = response.json()
                hasMore = result.get('hasMore', False)
                count = result.get('count', 0)
                items = result.get('items', [])
                all_items.extend(items)
                offset += limit
                total_results = result.get('totalResults', 0)
                pbar.total = total_results
                pbar.update(count)
                pbar.refresh()
            else:
                raise Exception(f'Error executing SuiteQL query: {response.status_code}, {response.text}')
                # hasMore = False
    # Convert results to DataFrame and return
    df = pd.DataFrame(all_items)
    return df


def read_csv_from_s3(
    bucket_name, 
    object_key, 
    s3_client, 
    encoding='utf-8', 
    is_csv_file=True, 
    low_memory = True, 
    dtype_str=False
):
    obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    file_size = obj['ContentLength']
    progress = tqdm(total=file_size, unit='B', unit_scale=True, desc=f'Downloading {object_key}')
    def stream_with_progress(bytes_io):
        while True:
            chunk = bytes_io.read(1024 * 1024)
            if not chunk:
                break
            progress.update(len(chunk))
            yield chunk
        progress.close()
    body = obj['Body']
    if is_csv_file:
        stream = stream_with_progress(body)
        csv_string = b''.join(stream).decode(encoding)
        csv_buffer = io.StringIO(csv_string)
        if dtype_str:
            df = pd.read_csv(csv_buffer, sep=',', quotechar='"', quoting=csv.QUOTE_ALL, low_memory=low_memory, dtype=str, na_values=[''], keep_default_na=False)
        else:    
            df = pd.read_csv(csv_buffer, sep=',', quotechar='"', quoting=csv.QUOTE_ALL, low_memory=low_memory)        
    else:
        stream = stream_with_progress(body)
        xlsx_data = b''.join(stream)
        xlsx_buffer = io.BytesIO(xlsx_data)
        df = pd.read_excel(xlsx_buffer, engine='openpyxl')
    return df


def clean_df(
    s3_client,
    s3_bucket_name,
    df,
    df_name,
    id_column=None,
    additional_date_columns=None,
    zip_code_columns=None,
    state_columns=None,
    keep_invalid_as_null=True,
    numeric_id=False, 
    just_useful_columns=False
):
    col_to_date = [col for col in df.columns if 'date' in col.lower()] + additional_date_columns
    col_to_date = list(set(col_to_date))
    for col in col_to_date:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    for col in set(df.columns)-set(id_column):
        df[col] = df[col].apply(lambda x: x.strip().upper() if isinstance(x, str) else x)
    if id_column:
        if numeric_id:
            invalid_mask = ~df[id_column].astype('str').apply(lambda x: x.str.isdigit()).any(axis=1)
            # invalid_mask = ~df[id_column].astype('str').str.isdigit()
            invalid_id = df[invalid_mask]
            df = df[~invalid_mask].copy()
            print(f"invalid {id_column} found and removed: {len(invalid_id)}")
            if len(invalid_id)>0:
                # upload_to_s3(s3_client = s3_client,  data = invalid_id, bucket_name = s3_bucket_name + '-c', object_key = f"{df_name}_invalid_{id_column}.csv", CreateS3Bucket=True)
                upload_to_s3(s3_client = s3_client,  data = invalid_id, bucket_name = s3_bucket_name + '-c', object_key = f"{df_name}_invalid_{re.sub(r'[^a-zA-Z0-9]', '_', '_'.join(id_column))}.csv", CreateS3Bucket=True)
        duplicated_mask = df[id_column].duplicated()
        duplicated_id = df[duplicated_mask]
        df = df[~duplicated_mask].copy()
        print(f"duplicated {id_column} found and removed: {len(duplicated_id)}")
        if len(duplicated_id)>0:
            # upload_to_s3(s3_client = s3_client,  data = duplicated_id, bucket_name = s3_bucket_name + '-c', object_key = f"{df_name}_duplicated_{id_column}.csv", CreateS3Bucket=True)
            upload_to_s3(s3_client = s3_client,  data = duplicated_id, bucket_name = s3_bucket_name + '-c', object_key = f"{df_name}_duplicated_{re.sub(r'[^a-zA-Z0-9]', '_', '_'.join(id_column))}.csv", CreateS3Bucket=True)
    if zip_code_columns:
        invalid_zip_codes = pd.DataFrame()
        valid_us_zip_regex = r"^\d{5}(\d{4})?$"
        valid_ca_zip_regex = r"^[A-Za-z]\d[A-Za-z](\d[A-Za-z]\d)?$"        
        for col in zip_code_columns:
            df[col] = df[col].astype('str').str.replace(' ','')
            df[col] = df[col].astype('str').str.replace('-','')            
            invalid_mask = ~(df[col].str.match(valid_us_zip_regex) | df[col].str.match(valid_ca_zip_regex))
            invalid_zip_codes = pd.concat([invalid_zip_codes, df[invalid_mask]], ignore_index=True)
            df = df[~invalid_mask].copy()
            df[col] = df[col].apply(lambda x: \
                                    x[0:5]+'-'+x[0:4] if isinstance(x, str) and re.match(valid_us_zip_regex, x) else \
                                    x[0:3]+' '+x[3:6] if isinstance(x, str) and re.match(valid_ca_zip_regex, x) else  \
                                    x)
        print(f"invalid_zip_codes found: {len(invalid_zip_codes)}")
        upload_to_s3(s3_client = s3_client,  data = invalid_zip_codes, bucket_name = s3_bucket_name + '-c', object_key = f"{df_name}_invalid_zip_codes.csv", CreateS3Bucket=True)
        if keep_invalid_as_null:
            for col in zip_code_columns:
                invalid_zip_codes[col] = np.nan
            df = pd.concat([df, invalid_zip_codes], ignore_index=True)        
    if state_columns:
        invalid_states = pd.DataFrame()
        valid_us_states = { "DC": "District of Columbia", "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming" }
        valid_ca_states = { "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba", "NB": "New Brunswick", "NL": "Newfoundland and Labrador", "NS": "Nova Scotia", "ON": "Ontario", "PE": "Prince Edward Island", "QC": "Quebec", "SK": "Saskatchewan", "NT": "Northwest Territories", "NU": "Nunavut", "YT": "Yukon" }
        for col in state_columns:
            df[col] = df[col].astype('str').str.replace(' ','')
            df[col] = df[col].astype('str').str.replace('-','')
            invalid_mask = ~df[col].isin(set(valid_us_states.keys()).union(valid_ca_states.keys()))
            invalid_states = pd.concat([invalid_states, df[invalid_mask]], ignore_index=True)
            df = df[~invalid_mask].copy()
        print(f"invalid_states found: {len(invalid_states)}")
        upload_to_s3(s3_client = s3_client,  data = invalid_states, bucket_name = s3_bucket_name + '-c', object_key = f"{df_name}_invalid_states.csv", CreateS3Bucket=True)
        if keep_invalid_as_null:
            for col in state_columns:
                invalid_states[col] = np.nan
            df = pd.concat([df, invalid_states], ignore_index=True)
    if just_useful_columns:
        useful_columns = find_useful_columns(df)
        print(f"{len(useful_columns)} useful variables found!")
        df = df[useful_columns]
    return df

def find_useful_columns(
    df
):
    useful_cols = [col for col in df.columns if not (df[col].isna().sum() == df.shape[0] or df[col].value_counts().iloc[0] == df.shape[0])]
    return useful_cols

def group(
    x, 
    quantile_values
):
    if pd.isnull(x):
        return None
    elif x <= quantile_values[1]:
        return f"{quantile_values[0]:03}-{quantile_values[1]:03}"
    elif x <= quantile_values[2]:
        return f"{quantile_values[1]+1:03}-{quantile_values[2]:03}"
    elif x <= quantile_values[3]:
        return f"{quantile_values[2]+1:03}-{quantile_values[3]:03}"
    elif x <= quantile_values[4]:
        return f"{quantile_values[3]+1:03}-{quantile_values[4]:03}"
    else:
        return f"{quantile_values[4]+1:03}+"

def find_unique_value_columns(
    dataframe
):
    unique_value_columns = []
    for column in dataframe.columns:
        if dataframe[column].nunique() == len(dataframe):
            unique_value_columns.append(column)
    return unique_value_columns

def write_file(
    filename, 
    data
):
    if os.path.isfile(filename):
        with open(filename, 'a', encoding='utf-8') as f:
            f.write('\n' + data)
    else:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(data)

def print_date_time():
    now = datetime.now()
    current_time = now.strftime("%D-%H:%M:%S")
    data = "Current Time = " + current_time
    return data

def correctCompleteDates(
    df, 
    orderStatusCol, 
    orderDateCol, 
    completeDateCol, 
    shipDateCol, 
    invoiceDateCol, 
    lastModDateCol, 
    postCompletionStatuses,
    fallback_to_order_date=True
):
    today = pd.Timestamp(datetime.today().date())

    for col in [orderDateCol, completeDateCol, shipDateCol, invoiceDateCol, lastModDateCol]:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        df[col] = df[col].mask(df[col] > today, today)

    postCompletionStatuses = set(postCompletionStatuses)
    def correctCompleteDate(row):
        orderStatus = row[orderStatusCol]
        orderDate = row[orderDateCol]
        completeDate = row[completeDateCol]
        arriveDate = row[shipDateCol]
        invoiceDate = row[invoiceDateCol]
        lastModDate = row[lastModDateCol]
        if completeDate >= orderDate:
            return completeDate

        if orderStatus in postCompletionStatuses:
            if arriveDate >= orderDate:
                return arriveDate
            if invoiceDate >= orderDate:
                return invoiceDate
            if lastModDate >= orderDate:
                return lastModDate
            if fallback_to_order_date:
                return orderDate
        else:
            return None
    df['CorrectedCompletedDate'] = df.apply(correctCompleteDate, axis=1)
    return df

def convert_to_int_or_keep(
    x
):
    try:
        return int(pd.to_numeric(x))
    except (ValueError, TypeError):
        return x

state_map = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
    'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
    'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO',
    'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
    'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
    'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
    'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
}

abbrev_map = {v: v for v in state_map.values()}
state_map.update(abbrev_map)

def extract_state(
    text
):
    if not isinstance(text, str):
        return None
    for key, value in state_map.items():
        match = re.search(rf'\b{key.lower()}\b', text.lower())
        if match:
            return value
    return None

def read_iif_from_s3(
    bucket_name, 
    object_key, 
    s3_client, 
    encoding='Windows-1252'
):

    iif_obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    file_size = iif_obj['ContentLength']   
    progress = tqdm(total=file_size, unit='B', unit_scale=True, desc=f'Downloading {object_key}')

    def stream_with_progress(bytes_io):
        while True:
            chunk = bytes_io.read(1024 * 1024)
            if not chunk:
                break
            progress.update(len(chunk))
            yield chunk
        progress.close()

    body = iif_obj['Body']
    stream = stream_with_progress(body)
    iif_string = b''.join(stream).decode(encoding)  
    iif_buffer = io.StringIO(iif_string)
    columns = [f'Column{i}' for i in range(1, 101)]
    df = pd.read_csv(iif_buffer, delimiter='\t', names=columns, encoding=encoding)

    return df

def clean_address(
    df
):
    def extract_address_name_city_state_zip(address):
        address = str(address)
        if address is None:
            return None, None, None, None 
        try:
            address = address.upper()
            us_zip_pattern = r'\b\d{5}\b'
            ca_zip_pattern = r'[A-Za-z]\d[A-Za-z]\s?\d[A-Za-z]\d'
            matches = re.findall(us_zip_pattern, address) or re.findall(ca_zip_pattern, address)
            zip_code = matches[-1]
            address = ''.join(re.split(zip_code, address)[:-1])
            valid_us_states = {'DISTRICT OF COLUMBIA': 'DC', 'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR', 'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE', 'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID', 'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS', 'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD', 'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS', 'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV', 'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK', 'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC', 'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT', 'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV', 'WISCONSIN': 'WI', 'WYOMING': 'WY'}
            valid_ca_states = {'ALBERTA': 'AB', 'BRITISH COLUMBIA': 'BC', 'MANITOBA': 'MB', 'NEW BRUNSWICK': 'NB', 'NEWFOUNDLAND AND LABRADOR': 'NL', 'NOVA SCOTIA': 'NS', 'ONTARIO': 'ON', 'PRINCE EDWARD ISLAND': 'PE', 'QUEBEC': 'QC', 'SASKATCHEWAN': 'SK', 'NORTHWEST TERRITORIES': 'NT', 'NUNAVUT': 'NU', 'YUKON': 'YT'}
            valid_states = {**valid_us_states, **valid_ca_states}
            state_pattern = re.compile(r'\b(' + '|'.join(re.escape(state) for state in valid_states.keys()) + r')\b', re.IGNORECASE)
            address = state_pattern.sub(lambda match: valid_states[match.group(0).upper()], address)
            state_pattern = r'\b[a-zA-Z]{2}\b'
            matches = re.findall(state_pattern, address)
            state = matches[-1]
            address = ''.join(re.split(state, address)[:-1])
            city = [i for i in address.strip().split(',') if i != ''][-1].strip()
            address = ''.join(re.split(city, address)[:-1])
            ship_name = [i for i in address.strip().split(',') if i != ''][0].strip()
            return ship_name, city, state, zip_code
        except:
            return None, None, None, None 

    addresses = {'BillAddressBlockAddr':'billingAddress', 'ShipAddressBlockAddr':'ShippingAddress', 'BADDR':'billingAddress', 'SADDR':'ShippingAddress', 'ADDR':'Address'}
    for key, value in addresses.items():
        AddressCols = [i for i in df.columns if key in i]
        if AddressCols:
            df[value] = df[AddressCols].agg(lambda x: ', '.join(x.dropna()), axis=1)
            df.drop(columns = AddressCols, inplace=True)
            if not df.empty:
                df[[f'{value}Name', f'{value}City', f'{value}State', f'{value}Zip']] = df[value].apply(extract_address_name_city_state_zip).to_list()
            else:
                df[[f'{value}Name', f'{value}City', f'{value}State', f'{value}Zip']] = None
    return df

def extract_lists(
    transactions, 
    table
):
    df = transactions.copy()
    columns = [ df[df['Column1'] == f'!{table}'][col].iloc[0] if not pd.isna(df[df['Column1'] == f'!{table}'][col].iloc[0]) else col for col in df.columns ]
    df.columns = columns
    df = df[df[f'!{table}'] == f'{table}']
    df = df[[i for i in df.columns if 'Column' not in i]].copy()
    df = clean_address(df)
    return df

def extract_transaction_header_line(
    transactions, 
    trns_type
):
    df = transactions.copy()
    df_columns = [ df[df['Column1'] == f'!TRNS'][col].item() if not pd.isna(df[df['Column1'] == f'!TRNS'][col].item()) else col for col in df.columns ]
    df_line_columns = [ df[df['Column1'] == f'!SPL'][col].item() if not pd.isna(df[df['Column1'] == f'!SPL'][col].item()) else col for col in df.columns ]
    df = df[df['Column3'] == f'{trns_type}']
    df = df[~df['Column2'].duplicated()].copy()
    for Col in ['Column2', 'Column9']:
        df.loc[:, Col] = df[Col].fillna('').apply(convert_to_int_or_keep).astype('str')
        df.loc[df['Column1']=='SPL', Col] = None
        df.loc[:, Col] = df[Col].ffill()
    df_line = df[df['Column1'] == 'SPL'].copy()
    df_line.columns = df_line_columns
    df_line = df_line[[i for i in df_line.columns if 'Column' not in i]].copy()
    if not df_line.empty:
        df_line = clean_address(df_line)
    df = df[df['Column1'] == 'TRNS'].copy()
    df.columns = df_columns
    df = df[[i for i in df.columns if 'Column' not in i]].copy()
    # if not df.empty:
    df = clean_address(df)
    return df, df_line

def replace_date(
    row, 
    date_col, 
    year_col=None, 
    month_col=None,
    day_col=None
):
    year = row[date_col].year
    month = row[date_col].month
    day = row[date_col].day
    if year_col and year_col in row:
        year = row[year_col]
    if month_col and month_col in row:
        month = row[month_col]
    if day_col and day_col in row:
        day = row[day_col]
    try:
        return row[date_col].replace(year=year, month=month, day=day)
    except ValueError:
        last_valid_day = (pd.Timestamp(f"{year}-{month}-01") + pd.offsets.MonthEnd(0)).day
        return row[date_col].replace(year=year, month=month, day=min(day, last_valid_day))

def wait_for_cluster_available(
    redshift_client,
    redshift_cluster_identifier
):
    waiter = redshift_client.get_waiter('cluster_available')
    try:
        prompt = f'{print_date_time()}\t\tWaiting for the Redshift cluster "{redshift_cluster_identifier}" to become available...'
        print(prompt)
        write_file('log.txt', f"{prompt}")
        waiter.wait(ClusterIdentifier=redshift_cluster_identifier)
        response = redshift_client.describe_clusters(ClusterIdentifier=redshift_cluster_identifier)
        cluster_status = response['Clusters'][0]['ClusterStatus']
        if cluster_status == 'available':
            prompt = f'{print_date_time()}\t\tCluster "{redshift_cluster_identifier}" is now available.'
            print(prompt)
            write_file('log.txt', f"{prompt}")
        else:
            prompt = f'{print_date_time()}\t\tCluster "{redshift_cluster_identifier}" is not available. Current status: {cluster_status}'
            print(prompt)
            write_file('log.txt', f"{prompt}")
            raise ValueError(f'Cluster "{redshift_cluster_identifier}" is not available. Current status: "{cluster_status}"')
    except Exception as e:
        prompt = f'{print_date_time()}\t\t[ERROR] Error waiting for cluster to become available: {e}'
        print(prompt)
        write_file('log.txt', f"{prompt}")

def create_iam_role(
    iam_client,
    role_name,
    trust_policy
):
    try:
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy)
        )
        prompt = f'{print_date_time()}\t\t[SUCCESS] Role "{role_name}" created!'
        print(prompt)
        write_file('log.txt', f"{prompt}")
        return response['Role']['Arn']
    except iam_client.exceptions.EntityAlreadyExistsException:
        prompt = f'{print_date_time()}\t\t[INFO] Role "{role_name}" already exists.'
        print(prompt)
        write_file('log.txt', f"{prompt}")
        return iam_client.get_role(RoleName=role_name)['Role']['Arn']

def attach_policies_to_role(
    iam_client,
    role_name, 
    role_policies
):
    for policy in role_policies:
        try:
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=f'arn:aws:iam::aws:policy/{policy}'
            )
            prompt = f'{print_date_time()}\t\tPolicy "{policy}" attached to role "{role_name}".'
            print(prompt)
            write_file('log.txt', f"{prompt}")
        except iam_client.exceptions.NoSuchEntityException as e:
            prompt = f'{print_date_time()}\t\t[ERROR] Error attaching policy "{policy}": {str(e)}'
            print(prompt)
            write_file('log.txt', f"{prompt}")

def associate_role_with_redshift(
    redshift_client,
    redshift_iam_role_arn, 
    redshift_cluster_identifier, 
    timeout=20, 
    check_interval=2
):
    try:
        response = redshift_client.describe_clusters(ClusterIdentifier=redshift_cluster_identifier)
        current_roles = response["Clusters"][0].get("IamRoles", [])
        role_associated = any(role['IamRoleArn'] == redshift_iam_role_arn for role in current_roles) 
        if role_associated:
            prompt = f'{print_date_time()}\t\t[INFO] Role "{redshift_iam_role_arn}" is already associated with the Redshift cluster "{redshift_cluster_identifier}".'
            print(prompt)
            write_file('log.txt', f"{prompt}")
        else:
            redshift_client.modify_cluster_iam_roles(
                ClusterIdentifier=redshift_cluster_identifier,
                AddIamRoles=[redshift_iam_role_arn]
            )
            prompt = f'{print_date_time()}\t\tAttempting to associate the role "{redshift_iam_role_arn}" with the Redshift cluster "{redshift_cluster_identifier}".'
            print(prompt)
            write_file('log.txt', f"{prompt}")
            wait_for_cluster_available(redshift_client, redshift_cluster_identifier) 
            elapsed_time = 0
            role_associated = False
            while elapsed_time < timeout and not role_associated:
                time.sleep(check_interval)
                elapsed_time += check_interval
                response = redshift_client.describe_clusters(ClusterIdentifier=redshift_cluster_identifier)
                updated_roles = response["Clusters"][0].get("IamRoles", [])
                role_associated = any(role['IamRoleArn'] == redshift_iam_role_arn for role in updated_roles)
                if role_associated:
                    prompt = f'{print_date_time()}\t\t[SUCCESS] Role "{redshift_iam_role_arn}" has been associated with the Redshift cluster "{redshift_cluster_identifier}"!'
                    print(prompt)
                    write_file('log.txt', f"{prompt}")
                else:
                    prompt = f'{print_date_time()}\t\t⏳ Waiting for IAM role "{redshift_iam_role_arn}" to be associated with the Redshift cluster "{redshift_cluster_identifier}". Retrying...'
                    print(prompt)
                    write_file('log.txt', f"{prompt}")
            if not role_associated:
                prompt = f'{print_date_time()}\t\t[ERROR] Timeout reached. Role "{redshift_iam_role_arn}" was not associated with the Redshift cluster "{redshift_cluster_identifier}" within {timeout} seconds.'
                print(prompt)
                write_file('log.txt', f"{prompt}")
    except redshift_client.exceptions.ClusterNotFoundFault:
        prompt = f'{print_date_time()}\t\t[ERROR] Redshift cluster "{redshift_cluster_identifier}" not found.'
        print(prompt)
        write_file('log.txt', f"{prompt}")
    except Exception as e:
        prompt = f'{print_date_time()}\t\t[ERROR] Error associating role: {str(e)}'
        print(prompt)
        write_file('log.txt', f"{prompt}")

def add_inbound_rule(
    redshift_client, 
    ec2_client, 
    redshift_cluster_identifier
):
    response = redshift_client.describe_clusters(ClusterIdentifier=redshift_cluster_identifier)
    security_group_id = response["Clusters"][0]["VpcSecurityGroups"][0]["VpcSecurityGroupId"]
    security_groups = ec2_client.describe_security_groups(GroupIds=[security_group_id])
    existing_rules = security_groups["SecurityGroups"][0]["IpPermissions"]
    rule_exists = any(
        rule["FromPort"] == 5439 and rule["ToPort"] == 5439 and rule["IpProtocol"] == "tcp"
        for rule in existing_rules
    )
    if not rule_exists:
        prompt = f'{print_date_time()}\t\tAdding inbound rule for security group to allow access from 0.0.0.0/0...'
        print(prompt)
        write_file('log.txt', f"{prompt}")   
        ec2_client.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpProtocol="tcp",
            FromPort=5439,
            ToPort=5439,
            CidrIp="0.0.0.0/0"
        )
        prompt = f'{print_date_time()}\t\t[SUCCESS] Inbound rule for 0.0.0.0/0 added to security group "{security_group_id}"!'
        print(prompt)
        write_file('log.txt', f"{prompt}")
    else:
        prompt = f'{print_date_time()}\t\t[INFO] Security group rule already exists.'
        print(prompt)
        write_file('log.txt', f"{prompt}")

def turn_on_case_sensitivity(
    redshift_client,
    redshift_cluster_identifier
):
    parameter_group_name = f'{redshift_cluster_identifier}-params'
    parameter_group_family = 'redshift-2.0'
    existing_groups = redshift_client.describe_cluster_parameter_groups()['ParameterGroups']
    group_names = [group['ParameterGroupName'] for group in existing_groups]
    if parameter_group_name not in group_names:  
        response = redshift_client.create_cluster_parameter_group(
            ParameterGroupName=parameter_group_name,
            ParameterGroupFamily=parameter_group_family,
            Description=f'Param group for {redshift_cluster_identifier}'
        )
        print(f"[SUCCESS] Created parameter group '{parameter_group_name}'!")

    else:
        print(f"[INFO] Parameter group '{parameter_group_name}' already exists.")
    wait_for_cluster_available(redshift_client, redshift_cluster_identifier)
    response = redshift_client.modify_cluster_parameter_group(
        ParameterGroupName=parameter_group_name,
        Parameters=[
            {
                'ParameterName': 'enable_case_sensitive_identifier',
                'ParameterValue': 'true',
                'ApplyType': 'static'
            }
        ]
    )
    print(f"Modified parameter group '{parameter_group_name}'")
    wait_for_cluster_available(redshift_client, redshift_cluster_identifier)
    cluster = redshift_client.describe_clusters(ClusterIdentifier=redshift_cluster_identifier)['Clusters'][0]
    current_parameter_group = cluster['ClusterParameterGroups'][0]['ParameterGroupName']
    if current_parameter_group != parameter_group_name:
        response = redshift_client.modify_cluster(
            ClusterIdentifier=redshift_cluster_identifier,
            ClusterParameterGroupName=parameter_group_name
        )
        print(f"Modified cluster '{redshift_cluster_identifier}' with parameter group '{parameter_group_name}'")
        response = redshift_client.reboot_cluster(ClusterIdentifier=redshift_cluster_identifier)
    else:
        print(f"[INFO] Cluster '{redshift_cluster_identifier}' already has the parameter group '{parameter_group_name}' associated.")
    wait_for_cluster_available(redshift_client, redshift_cluster_identifier)

def upload_to_redshift(
    s3_client,
    redshift_client,
    iam_client,
    ec2_client,
    trust_policy,
    role_policies,
    s3_bucket_names,
    redshift_cluster_identifier,
    redshift_db_name,
    redshift_master_username,
    redshift_master_password,
    redshift_users,
    role_name,
    redshift_node_type,
    redshift_cluster_type,
    redshift_number_of_nodes,
    createRedshiftCluster=False,
    max_allowed_length= 870
):
    try:
        redshift_iam_role_arn = create_iam_role(iam_client, role_name, trust_policy)
        attach_policies_to_role(iam_client, role_name, role_policies)
        response = redshift_client.describe_clusters(ClusterIdentifier=redshift_cluster_identifier)
        cluster_status = response["Clusters"][0]["ClusterStatus"]
        prompt = f'{print_date_time()}\t\t[INFO] Cluster "{redshift_cluster_identifier}" exists. Status: {cluster_status}'
        print(prompt)
        write_file('log.txt' , f"{prompt}")
    except redshift_client.exceptions.ClusterNotFoundFault:
        if createRedshiftCluster:
            prompt = f'{print_date_time()}\t\tCluster "{redshift_cluster_identifier}" not found. Creating a new one...'
            print(prompt)
            write_file('log.txt' , f"{prompt}")
            redshift_client.create_cluster(
                ClusterIdentifier=redshift_cluster_identifier,
                NodeType=redshift_node_type,
                ClusterType=redshift_cluster_type,
                NumberOfNodes=redshift_number_of_nodes,
                DBName=redshift_db_name,
                MasterUsername=redshift_master_username,
                MasterUserPassword=redshift_master_password,
                PubliclyAccessible=True
            )
            wait_for_cluster_available(redshift_client, redshift_cluster_identifier)
            prompt = f'{print_date_time()}\t\tCluster "{redshift_cluster_identifier}" is now available.'
            print(prompt)
            write_file('log.txt' , f"{prompt}")
        else:
            raise Exception(f'Cluster "{redshift_cluster_identifier}" does not exist and createRedshiftCluster=False. Aborting.')
    add_inbound_rule(redshift_client, ec2_client, redshift_cluster_identifier)
    associate_role_with_redshift(redshift_client, redshift_iam_role_arn, redshift_cluster_identifier)
    turn_on_case_sensitivity(redshift_client, redshift_cluster_identifier)
    response = redshift_client.describe_clusters(ClusterIdentifier=redshift_cluster_identifier)
    cluster_endpoint = response["Clusters"][0]["Endpoint"]["Address"]
    try:
        conn = psycopg2.connect(
            dbname=redshift_db_name,
            user=redshift_master_username,
            password=redshift_master_password,
            host=cluster_endpoint,
            port=5439
        )
        cur = conn.cursor()
        prompt = f'{print_date_time()}\t\t[SUCCESS] Connected to Redshift!'
        print(prompt)
        write_file('log.txt' , f"{prompt}")
        enable_case_sensitive_query = 'SET enable_case_sensitive_identifier TO true;'
        cur.execute(enable_case_sensitive_query)
        conn.commit()
        enable_case_sensitive_query = 'SET enable_case_sensitive_identifier TO on;'
        cur.execute(enable_case_sensitive_query)
        conn.commit()
        prompt = f'{print_date_time()}\t\tCase sensitivity enabled for this session.'
        print(prompt)
        write_file('log.txt' , f"{prompt}")
    except Exception as e:
        raise Exception(f'[ERROR] Failed to connect to Redshift: {e}')
    for bucket in s3_bucket_names:
        prompt = f'{print_date_time()}\t\tScanning S3 bucket: "{bucket}"'
        print(prompt)
        write_file('log.txt' , f"{prompt}")
        try:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix="")
            if "Contents" not in response:
                prompt = f'{print_date_time()}\t\tNo files found in bucket "{bucket}". Skipping...'
                print(prompt)
                write_file('log.txt' , f"{prompt}")
                continue
            csv_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]
            if not csv_files:
                prompt = f'{print_date_time()}\t\tNo CSV files found in bucket "{bucket}". Skipping...'
                print(prompt)
                write_file('log.txt' , f"{prompt}")
                continue
            prompt = f'{print_date_time()}\t\tFound {len(csv_files)} CSV file(s) in bucket "{bucket}". Uploading to Redshift...'
            print(prompt)
            write_file('log.txt' , f"{prompt}")
            for csv_file in csv_files:
                s3_path = f's3://{bucket}/{csv_file}'
                table_name = (bucket + '-' + csv_file.split('.csv')[0])
                check_table_query = f'''
                SELECT * FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
                '''
                cur.execute(check_table_query)
                result = cur.fetchall()
                if table_name in [row[2] for row in result]:
                    prompt = f'{print_date_time()}\t\t[INFO] Table "{table_name}" exists. Dropping it...'
                    print(prompt)
                    write_file('log.txt' , f"{prompt}")
                    drop_table_query = f'DROP TABLE "{table_name}";'
                    cur.execute(drop_table_query)
                    conn.commit()
                    prompt = f'{print_date_time()}\t\t[SUCCESS] Table "{table_name}" dropped!'
                    print(prompt)
                    write_file('log.txt' , f"{prompt}")
                df = read_csv_from_s3(s3_client = s3_client, bucket_name = bucket, object_key = csv_file, dtype_str=True)
                if df.empty:
                    prompt = f'{print_date_time()}\t\tWarning: DataFrame is empty. Creating a table with default column structure.'
                    print(prompt)
                    write_file('log.txt' , f"{prompt}")
                    create_table_query = f'CREATE TABLE "{table_name}" ('
                    create_table_query += ", ".join([f'"{col}" TEXT' for col in df.columns])
                    create_table_query += ");"
                else:
                    global max_lengths
                    max_lengths = df.astype('str').apply(lambda x: x.str.encode('utf-8').str.len().max()).fillna(0).astype('int')
                    max_lengths = max_lengths.replace(0,1)
                    cols_to_truncate = max_lengths[max_lengths > max_allowed_length].index.tolist()
                    # for col in cols_to_truncate:
                    #     df[col] = df[col].astype('str').apply(lambda x: truncate_with_etc_2(x, max_allowed_length))
                    #     max_lengths[col] = max_allowed_length
                    create_table_query = f'CREATE TABLE "{table_name}" ('
                    create_table_query += ", ".join([f'"{col}" VARCHAR({length})' for col, length in max_lengths.items()])
                    create_table_query += ");"
                prompt = f'{print_date_time()}\t\tCreating table "{table_name}"...'
                print(prompt)
                write_file('log.txt' , f"{prompt}")
                cur.execute(create_table_query)
                conn.commit()
                prompt = f'{print_date_time()}\t\t[SUCCESS] Table "{table_name}" created!'
                print(prompt)
                write_file('log.txt' , f"{prompt}")
                copy_query = f"""
                COPY "{table_name}"
                FROM '{s3_path}'
                IAM_ROLE '{redshift_iam_role_arn}'
                CSV
                IGNOREHEADER 1
                DELIMITER ','
                QUOTE '"'
                ;
                """
                try:
                    prompt = f'{print_date_time()}\t\tUploading {csv_file} to Redshift table "{table_name}"...'
                    print(prompt)
                    write_file('log.txt' , f"{prompt}")
                    cur.execute(copy_query)
                    conn.commit()
                    prompt = f'{print_date_time()}\t\t[SUCCESS] Uploaded {csv_file} to Redshift table "{table_name}"!'
                    print(prompt)
                    write_file('log.txt' , f"{prompt}")
                except Exception as e:
                    prompt = f'{print_date_time()}\t\t[ERROR] Error uploading {csv_file}: {e}'
                    print(prompt)
                    write_file('log.txt' , f"{prompt}")
                    raise
        except Exception as e:
            prompt = f'{print_date_time()}\t\t[ERROR] Error uploading files in bucket "{bucket}": {e}'
            print(prompt)
            write_file('log.txt' , f"{prompt}")
            raise
            
    for redshift_user in redshift_users:
        try:
            create_user_query = f"""CREATE USER "{redshift_user['username']}" WITH PASSWORD '{redshift_user['password']}';"""
            cur.execute(create_user_query)
            conn.commit()
            prompt = f'{print_date_time()}\t\t[SUCCESS] User {redshift_user["username"]} created successfully.'
            print(prompt)
            write_file('log.txt', f"{prompt}")
        except DuplicateObject:
            conn.rollback()
            prompt = f'{print_date_time()}\t\t[INFO] User {redshift_user["username"]} already exists. Skipping creation.'
            print(prompt)
            write_file('log.txt', f"{prompt}")
        grant_usage_query = f"""GRANT USAGE ON SCHEMA public TO "{redshift_user['username']}";"""
        cur.execute(grant_usage_query)
        conn.commit()
        for table in redshift_user['access']:
            try:
                grant_select_query = f"""GRANT SELECT ON TABLE public."{table}" TO "{redshift_user['username']}";"""
                cur.execute(grant_select_query)
                conn.commit()
                prompt = f'{print_date_time()}\t\t[SUCCESS] Granted SELECT on {table} to {redshift_user["username"]}.'
                print(prompt)
                write_file('log.txt', f"{prompt}")
            except UndefinedTable:
                conn.rollback()
                prompt = f'{print_date_time()}\t\t[ERROR] Table "{table}" does not exist. Skipping grant.'
                print(prompt)
                write_file('log.txt', f"{prompt}")
        prompt = f'{print_date_time()}\t\tPermissions granted successfully for {redshift_user["username"]}.'
        print(prompt)
        write_file('log.txt', f"{prompt}")

    cur.close()
    conn.close()
    wait_for_cluster_available(redshift_client, redshift_cluster_identifier)
    response = redshift_client.reboot_cluster(ClusterIdentifier=redshift_cluster_identifier)
    prompt = f'{print_date_time()}\t\t🚀 Upload process completed.'
    print(prompt)
    write_file('log.txt' , f"{prompt}")
