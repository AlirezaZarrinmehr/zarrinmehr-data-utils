#!/usr/bin/env python
# coding: utf-8

# # Zarrinmehr Data Utilities — Python Toolkit for Data Integration and ETL

# A curated collection of Python utility functions for data engineers and analysts.
# 

# ## Libraries

# In[1]:


import os
from datetime import date, timedelta, datetime
import time
import requests
import pandas as pd
import warnings
import boto3
from tqdm import tqdm
import io
import csv
import re
import numpy as np
import pytz
import json
import pyodbc
from requests_oauthlib import OAuth1
import psycopg2


# ## Functions

# In[ ]:


'''
clean_df(
clean_df(s3_client = s3_client, s3_bucket_name = s3_bucket_name,

read_csv_from_s3(
read_csv_from_s3(s3_client = s3_client, 

upload_to_s3(
upload_to_s3(s3_client = s3_client, data = 
'''


# In[2]:


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

def load_data_via_query(
    sql_query, 
    connection_string, 
    chunksize=1000
):
    print(f"Running {sql_query}")
    chunks = []
    with pyodbc.connect(connection_string) as conn:
        total_rows = pd.read_sql_query("SELECT COUNT(*) FROM ({}) subquery".format(sql_query), conn).iloc[0, 0]
        total_chunks = (total_rows // chunksize) + (total_rows % chunksize > 0)
        for chunk in tqdm(pd.read_sql_query(sql_query, conn, chunksize=chunksize), total=total_chunks):
            chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    df.columns = df.columns.str.title()
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

def upload_to_s3(
    data, 
    bucket_name, 
    object_key, 
    s3_client, 
    CreateS3Bucket=False
):
    
    if CreateS3Bucket:
        try:
            buckets = pd.DataFrame(s3_client.list_buckets()["Buckets"])
            if bucket_name not in buckets.Name.to_list():
                s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': AWS_REGION})
                prompt = f'{print_date_time()}\t\tBucket "{bucket_name}" created successfully'
                print(prompt)
                write_file('log.txt' , f"{prompt}")
            else:
                prompt = f'{print_date_time()}\t\tBucket "{bucket_name}" already exists'
                print(prompt)
                write_file('log.txt' , f"{prompt}")

        except Exception as e:
            prompt = f'{print_date_time()}\t\tFailed to create bucket "{bucket_name}". Error: {str(e)}.'
            print(prompt)
            write_file('log.txt' , f"{prompt}")

    clean_data = data.copy()
    # for col in clean_data.columns:
    for col in clean_data.select_dtypes(include=['object', 'string']).columns:
        clean_data[col] = clean_data[col].fillna('').astype(str).str.replace(r'\r\n|\r|\n', ' ', regex=True)

    csv_buffer = io.StringIO()
    clean_data.to_csv(csv_buffer, index=False, sep=',', quotechar='"', quoting=csv.QUOTE_ALL, escapechar='\\', encoding='utf-8')
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
            invalid_mask = ~df[id_column].astype(str).apply(lambda x: x.str.isdigit()).any(axis=1)
            # invalid_mask = ~df[id_column].astype(str).str.isdigit()
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
            df[col] = df[col].astype(str).str.replace(' ','')
            df[col] = df[col].astype(str).str.replace('-','')            
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
            df[col] = df[col].astype(str).str.replace(' ','')
            df[col] = df[col].astype(str).str.replace('-','')
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
    df , 
    orderStatusCol, 
    orderDateCol, 
    completeDateCol, 
    shipDateCol, 
    invoiceDateCol, 
    lastModDateCol, 
    postCompletionStatuses
):
    df[orderDateCol] = pd.to_datetime(df[orderDateCol], errors='coerce')
    df[completeDateCol] = pd.to_datetime(df[completeDateCol], errors='coerce')
    df[shipDateCol] = pd.to_datetime(df[shipDateCol], errors='coerce')
    df[invoiceDateCol] = pd.to_datetime(df[invoiceDateCol], errors='coerce')
    df[lastModDateCol] = pd.to_datetime(df[lastModDateCol], errors='coerce')                           
    def correctCompleteDate(row):
        orderStatus = row[orderStatusCol]
        orderDate = row[orderDateCol]
        completeDate = row[completeDateCol]
        arriveDate = row[shipDateCol]
        invoiceDate = row[invoiceDateCol]
        lastModDate = row[lastModDateCol]
        if completeDate >= orderDate:
            return completeDate
        elif completeDate < orderDate and arriveDate >= orderDate:
            return arriveDate
        elif completeDate < orderDate and invoiceDate >= orderDate:
            return invoiceDate
        elif orderStatus in postCompletionStatuses and lastModDate >= orderDate:
            return lastModDate
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
        prompt = f'{print_date_time()}\t\tError waiting for cluster to become available: {e}'
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
        prompt = f'{print_date_time()}\t\tRole "{role_name}" created successfully.'
        print(prompt)
        write_file('log.txt', f"{prompt}")
        return response['Role']['Arn']
    except iam_client.exceptions.EntityAlreadyExistsException:
        prompt = f'{print_date_time()}\t\t⚠️ Role "{role_name}" already exists.'
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
            prompt = f'{print_date_time()}\t\tError attaching policy "{policy}": {str(e)}'
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
            prompt = f'{print_date_time()}\t\t⚠️ Role "{redshift_iam_role_arn}" is already associated with the Redshift cluster "{redshift_cluster_identifier}".'
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
                    prompt = f'{print_date_time()}\t\t✅ Role "{redshift_iam_role_arn}" has been successfully associated with the Redshift cluster "{redshift_cluster_identifier}".'
                    print(prompt)
                    write_file('log.txt', f"{prompt}")
                else:
                    prompt = f'{print_date_time()}\t\t⏳ Waiting for IAM role "{redshift_iam_role_arn}" to be associated with the Redshift cluster "{redshift_cluster_identifier}". Retrying...'
                    print(prompt)
                    write_file('log.txt', f"{prompt}")
            if not role_associated:
                prompt = f'{print_date_time()}\t\t❌ Timeout reached. Role "{redshift_iam_role_arn}" was not associated with the Redshift cluster "{redshift_cluster_identifier}" within {timeout} seconds.'
                print(prompt)
                write_file('log.txt', f"{prompt}")
    except redshift_client.exceptions.ClusterNotFoundFault:
        prompt = f'{print_date_time()}\t\tError: Redshift cluster "{redshift_cluster_identifier}" not found.'
        print(prompt)
        write_file('log.txt', f"{prompt}")
    except Exception as e:
        prompt = f'{print_date_time()}\t\tError associating role: {str(e)}'
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
        prompt = f'{print_date_time()}\t\t✅ Inbound rule for 0.0.0.0/0 added to security group "{security_group_id}".'
        print(prompt)
        write_file('log.txt', f"{prompt}")
    else:
        prompt = f'{print_date_time()}\t\t⚠️ Security group rule already exists, skipping.'
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
        print(f"Created parameter group '{parameter_group_name}'")

    else:
        print(f"Parameter group '{parameter_group_name}' already exists. Skipping creation.")
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
        print(f"Cluster '{redshift_cluster_identifier}' already has the parameter group '{parameter_group_name}' associated.")
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
        prompt = f'{print_date_time()}\t\tCluster "{redshift_cluster_identifier}" exists. Status: {cluster_status}'
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
        prompt = f'{print_date_time()}\t\tConnected to Redshift successfully.'
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
        raise Exception(f'Failed to connect to Redshift: {e}')
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
                    prompt = f'{print_date_time()}\t\tTable "{table_name}" exists. Dropping it...'
                    print(prompt)
                    write_file('log.txt' , f"{prompt}")
                    drop_table_query = f'DROP TABLE "{table_name}";'
                    cur.execute(drop_table_query)
                    conn.commit()
                    prompt = f'{print_date_time()}\t\t✅ Table "{table_name}" dropped.'
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
                    max_lengths = df.astype(str).apply(lambda x: x.str.encode('utf-8').str.len().max()).fillna(0).astype(int)
                    max_lengths = max_lengths.replace(0,1)
                    cols_to_truncate = max_lengths[max_lengths > max_allowed_length].index.tolist()
                    # for col in cols_to_truncate:
                    #     df[col] = df[col].astype(str).apply(lambda x: truncate_with_etc(x, max_allowed_length))
                    #     max_lengths[col] = max_allowed_length
                    create_table_query = f'CREATE TABLE "{table_name}" ('
                    create_table_query += ", ".join([f'"{col}" VARCHAR({length})' for col, length in max_lengths.items()])
                    create_table_query += ");"
                prompt = f'{print_date_time()}\t\tCreating table "{table_name}"...'
                print(prompt)
                write_file('log.txt' , f"{prompt}")
                cur.execute(create_table_query)
                conn.commit()
                prompt = f'{print_date_time()}\t\t✅ Table "{table_name}" created.'
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
                    prompt = f'{print_date_time()}\t\t✅ Successfully uploaded {csv_file} to Redshift table "{table_name}".'
                    print(prompt)
                    write_file('log.txt' , f"{prompt}")
                except Exception as e:
                    prompt = f'{print_date_time()}\t\t❌ Error uploading {csv_file}: {e}'
                    print(prompt)
                    write_file('log.txt' , f"{prompt}")
                    raise
        except Exception as e:
            prompt = f'{print_date_time()}\t\t❌ Error uploading files in bucket "{bucket}": {e}'
            print(prompt)
            write_file('log.txt' , f"{prompt}")
            raise
    cur.close()
    conn.close()
    response = redshift_client.reboot_cluster(ClusterIdentifier=redshift_cluster_identifier)
    prompt = f'{print_date_time()}\t\t🚀 Upload process completed.'
    print(prompt)
    write_file('log.txt' , f"{prompt}")


# In[3]:


# Use Case : 
# from zarrinmehrlib import *

