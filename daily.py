import mlb
from  big_query_utils import duplicate_rows_check
from datetime import date, timedelta

if __name__ == '__main__':
    today = date.today()

    # Statcast
    mlb.add_statcast_data_to_big_query(today - timedelta(days = 1)) # Statcast data from yesterday's MLB games

    # Injuries
    mlb.update_injury_data_in_big_query()

    # Data Quality Check
    duplicate_rows_check()