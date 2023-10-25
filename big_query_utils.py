import os
import json
from google.oauth2 import service_account
import pandas as pd

if os.path.isfile('pjb-sports-data-4ad8cbc89360.json'):
    with open('pjb-sports-data-4ad8cbc89360.json') as f:
        os.environ['GOOGLE_CLOUD_API_KEY'] = f.read()
credentials = service_account.Credentials.from_service_account_info(json.loads(os.environ['GOOGLE_CLOUD_API_KEY']))
print(f'Successfully connected to Google Cloud project "', credentials.project_id, '" using service account "', credentials.service_account_email,
      '"...', sep = '')

# ------------------------------------------------------------------------------------------------ #
# ----------------------------------------- pandas-gbq: ------------------------------------------ #
# ----------------- https://cloud.google.com/bigquery/docs/pandas-gbq-migration ------------------ #
# ------------------------------------------------------------------------------------------------ #
def query_to_df(query: str):
    return pd.read_gbq(query, project_id = credentials.project_id, dialect = 'standard', credentials = credentials)

def add_df_rows_to_table(table: str, df: pd.DataFrame):
    df.to_gbq(table, project_id = credentials.project_id, if_exists = 'append', credentials = credentials)
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #

def duplicate_rows_check():
    table_primary_keys = {
        'mlb.statcast_games': ['game_pk'],
        'mlb.statcast_at_bats': ['game_pk', 'at_bat_number'],
        'mlb.statcast_pitches': ['game_pk', 'at_bat_number', 'pitch_number'],
        'mlb.fangraphs_injuries': ['playerId', 'season', 'date', 'injurySurgery']
    }
    query_str_list = list()
    for table_name, keys in table_primary_keys.items():
        query_str_list.append(f'''
            SELECT
                "{table_name}" AS table_name,
                COUNT(*) AS duplicate_rows
            FROM
                (
                    SELECT
                        0 AS a
                    FROM
                        `{table_name}`
                    GROUP BY
                        {','.join(keys)}
                    HAVING
                        COUNT(*) > 1
                )
        ''')
    query_str = ' UNION ALL '.join(query_str_list)
    duplicate_rows_df = query_to_df(query_str)
    print(duplicate_rows_df)
    if duplicate_rows_df.duplicate_rows.any():
        raise Exception('There are duplicate entries in the "mlb" dataset. Check the printed dataframe for more information.')