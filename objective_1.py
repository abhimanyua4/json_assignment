import json
import pandas as pd
import os
from datetime import datetime
import re

def id_validation(x):
    x = match(x)
    if len(x) == 36:
        return x
    else:
        return -1

def match(str1):
    if re.match("^[A-Z0-9-]*$",str1):
        return str1
    else:
        return -1

def get_chunk_size(lines):
    x = lines
    chunk_size = 1
    while x > 4000:
        chunk_size = x // len(str(x))
        x = chunk_size

    return chunk_size

def extract_data_from_json(json_file_path):

    count_lines = sum(1 for line in open(json_file_path))
    #print (count_lines)

    json_obj = pd.read_json(json_file_path, chunksize=get_chunk_size(count_lines), lines=True)
    df = json_obj.read()
    return df

def filter_dataframe(df):

    #Extracted the rows with non values
    nan_df = df[df.isnull().any(axis=1)]
    tmp1 = list(nan_df.isna().index)
    df_out = df.drop(tmp1)

    return df_out , nan_df

def schema_validator(df, relv_col):
    # all_columns = cols
    df_out, df_drop = filter_dataframe(df)
    df_out = df_out.reset_index().drop(columns=['index'], axis=1)
    df_drop = df_drop.fillna('nan')
    df_temp = df_drop[relv_col]

    lst1 = []
    for i in range(len(df_temp)):
        if 'nan' not in list(dict(df_temp.iloc[i]).values()):
            lst1.append(i)

    for item in lst1:
        df_out.loc[len(df_out.index)] = dict(df_drop.iloc[item])

    for item in lst1:
        df_drop = df_drop.drop(item)

    return df_out, df_drop

def column_validator(df):
    df['id'] = df['id'].apply(lambda x: id_validation(x))
    df['anonymous_id'] = df['anonymous_id'].apply(lambda x: id_validation(x))
    df['context_device_ad_tracking_enabled'] = df['context_device_ad_tracking_enabled'].apply(
        lambda x: True if x == True else False)
    df['context_network_wifi'] = df['context_network_wifi'].apply(lambda x: True if x == True else False)
    df['event'] = df['event'].astype(str)
    df['sent_at'] = pd.to_datetime(df['sent_at'])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['original_timestamp'] = pd.to_datetime(df['original_timestamp'])

    return df

if __name__ == '__main__':

    pd.options.display.max_columns = None
    pd.options.display.max_rows = None

    current_dir = os.getcwd()
    dir = os.path.join(current_dir,str(datetime.now().strftime('%Y-%m-%d')))
    if not os.path.exists(dir):
        os.makedirs(dir)

    timestamp = datetime.now().strftime('%H_%M_%d_%m_%Y')
    logfile = os.getcwd() + '/logfolder/' + 'logfolder_' + timestamp + '.txt'

    f = open(logfile,'w')
    f.write('----- Start of the program -----')
    f.write('\n')
    f.write('Program starts at : {0}'.format(timestamp))
    f.write('\n')
    json_file_path = os.getcwd() + '/input.json'

    df = extract_data_from_json(json_file_path)
    all_cols = list(df.columns.values)

    relv_col = ['context_device_manufacturer', 'context_os_name', 'context_network_carrier',
                'event_text', 'context_traits_taxfix_language', 'context_network_wifi', 'received_at',
                'context_device_type', 'context_locale', 'event', 'timestamp', 'anonymous_id', 'context_device_model',
                'id', 'original_timestamp', 'sent_at']

    temp = []
    for item in relv_col:
        if item in all_cols:
            temp.append(True)
        else:
            temp.append(False)
    output = list(set(temp))[0]

    if output == True:
        df = column_validator(df)
        if len(df) > 0:
            df_out, df_drop = schema_validator(df,relv_col)

            df_drop.to_csv(str(datetime.now().strftime('%Y-%m-%d')) + '/drop_files.csv')
            df_out.to_csv(str(datetime.now().strftime('%Y-%m-%d')) + '/process_files.csv')

            if len(df_drop) > 0:
                for item in list(df_drop.index):
                    f.write('\n')
                    f.write(datetime.now().strftime('%H:%M %d:%m:%Y')+':'+str(dict(df_drop.loc[item])))
                    f.write('\n')
            else:
                f.write(datetime.now().strftime('%H:%M %d:%m:%Y')+':'+'No data_drop JSON object is present')
        else:
            f.write(datetime.now().strftime('%H:%M %d:%m:%Y')+':'+'Empty JSON File')
    else:
        f.write(datetime.now().strftime('%H:%M %d:%m:%Y')+'Mandatory data fields are absent')

    f.close()