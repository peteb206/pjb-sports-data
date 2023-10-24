import mlb
from  big_query_utils import query_to_df
from datetime import date, timedelta

if __name__ == '__main__':
    mlb.add_statcast_data_to_big_query(date.today() - timedelta(days = 1)) # Statcast data from yesterday's MLB games
    duplicate_rows_df = query_to_df(mlb.queries.duplicate_rows)
    if duplicate_rows_df.duplicate_rows.sum() == 0:
        print(duplicate_rows_df)
        raise Exception('There are duplicate entries in the "mlb" dataset. Check the printed dataframe for more information.')