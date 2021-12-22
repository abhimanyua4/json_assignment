from objective_1 import extract_data_from_json , get_chunk_size
import os
import pandas as pd
pd.options.mode.chained_assignment = None


if __name__ == '__main__':
    pd.options.display.max_rows = None
    pd.options.display.max_columns = None

    json_file_path = os.getcwd() + '/input.json'
    df = extract_data_from_json(json_file_path)
    #print (df)

    df_new = df[['original_timestamp','event']]
    df_new['date'] = df_new['original_timestamp'].apply(lambda x: pd.to_datetime(x).date().strftime('%Y-%m-%d'))

    # without adding new column
    new_df = pd.DataFrame(df_new[['date','event']].value_counts())
    new_df = new_df.rename(columns={0:'count'})
    print (new_df)
    new_df.to_csv('objective_2.csv')

    # with adding a new column
    df_new['count'] = 1
    temp_df = df_new[['date', 'event', 'count']].groupby(['date', 'event'], as_index=False).count()
    temp_df.to_csv('objective_2_new.csv')
    print (temp_df)