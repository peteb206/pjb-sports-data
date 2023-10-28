import mlb
import big_query_utils
from datetime import date, timedelta
from tests import test

if __name__ == '__main__':
    today = date.today()

    # Statcast
    if today not in big_query_utils.query_to_df(big_query_utils.queries.existing_game_dates()).game_date:
        mlb.add_statcast_data_to_big_query(today - timedelta(days = 1)) # Statcast data from yesterday's MLB games

    # Injuries
    mlb.update_injury_data_in_big_query()

    # Tests
    test()