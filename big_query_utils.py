import os
import json
from google.oauth2 import service_account
import pandas as pd
from datetime import date

# ------------------------------------------------------------------------------------------------ #
# ------------------------------- PJB Sports Data BigQuery Project ------------------------------- #
# ------- https://console.cloud.google.com/bigquery?hl=en&project=pjb-sports-data&ws=!1m0 -------- #
# ------------------------------------------------------------------------------------------------ #
if os.path.isfile('pjb-sports-data-4ad8cbc89360.json'):
    with open('pjb-sports-data-4ad8cbc89360.json') as f:
        os.environ['GOOGLE_CLOUD_API_KEY'] = f.read()
credentials = service_account.Credentials.from_service_account_info(json.loads(os.environ['GOOGLE_CLOUD_API_KEY']))
print(f'Successfully connected to Google Cloud project "', credentials.project_id, '" using service account "', credentials.service_account_email,
      '"...', sep = '')
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
# ----------------------------------------- pandas-gbq: ------------------------------------------ #
# ----------------- https://cloud.google.com/bigquery/docs/pandas-gbq-migration ------------------ #
# ------------------------------------------------------------------------------------------------ #
def query_to_df(query: str):
    return pd.read_gbq(query, project_id = credentials.project_id, dialect = 'standard', credentials = credentials)

def add_df_rows_to_table(table: str, df: pd.DataFrame):
    if len(df.index) > 0:
        df.columns = [col.replace('.', '_') for col in df.columns]
        df.to_gbq(table, project_id = credentials.project_id, if_exists = 'append', credentials = credentials)
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
# -------------------------------------------- Tables -------------------------------------------- #
# ------------------------------------------------------------------------------------------------ #
table_primary_keys = {
    'mlb.statcast_pitches': ['game_pk', 'at_bat_number', 'pitch_number'],
    'mlb.fangraphs_injuries': ['playerId', 'season', 'date', 'injurySurgery']
}
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------- Queries -------------------------------------------- #
# ------------------------------------------------------------------------------------------------ #
class queries:
    duplicate_rows_check = ''
    for i, (table_name, primary_keys) in enumerate(table_primary_keys.items()):
        if i > 0:
            duplicate_rows_check += ' UNION ALL '
        duplicate_rows_check += f'''
            SELECT
                "{table_name}" `Table`,
                COUNT(*) `Duplicate Rows`
            FROM
                (
                    SELECT
                        0 a
                    FROM
                        `{table_name}`
                    GROUP BY
                        {', '.join(primary_keys)}
                    HAVING
                        COUNT(*) > 1
                )
        '''

    statcast_pitches_by_year = '''
        SELECT
            game_year Year,
            COUNT(*) `Big Query Pitches`
        FROM
            `mlb.statcast_pitches`
        WHERE
            game_type = "R"
        GROUP BY
            game_year
    '''

    def existing_game_dates(year: int | None = None, game_type: str | None = None):
        where_clause = ''
        if type(year) == int:
            where_clause = f'WHERE game_year = {year}'
        if type(game_type) == str:
            if where_clause == '':
                where_clause = f'WHERE game_type = "{game_type}"'
            else:
                where_clause += f' AND game_type = "{game_type}"'
        return f'''
            SELECT
                DISTINCT game_date
            FROM
                `mlb.statcast_pitches`
            {where_clause}
            ORDER BY
                game_date
        '''

    clear_fangraphs_injuries = '''
        DELETE
        FROM
            `mlb.fangraphs_injuries`
        WHERE
            true
    '''

    def statcast(start_date: date, end_date: date):
        return f'''
            SELECT
                *
            FROM
                `mlb.statcast_pitches` pitches
            WHERE
                game_date >= "{start_date.strftime('%Y-%m-%d')}" AND game_date <= "{end_date.strftime('%Y-%m-%d')}"
        '''
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #