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