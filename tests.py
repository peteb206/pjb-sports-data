from big_query_utils import queries, query_to_df
import requests
import pandas as pd
from random import randint
from datetime import date
import json
import re

def test(passed: bool, message: str) -> bool:
    if passed:
        print('RESULT:', u'\u2705', 'PASS')
    else:
        print(message)
        print('RESULT:', u'\u274C', 'FAIL')
    return passed

# ------------------------------------------------------------------------------------------------ #
# ----------------------------------------- Data Quality ----------------------------------------- #
# ------------------------------------------------------------------------------------------------ #
def duplicate_rows_check():
    print('\nDuplicate Rows check:')
    duplicate_rows_df = query_to_df(queries.duplicate_rows_check).set_index('Table')
    duplicate_rows_df['Test'] = duplicate_rows_df.apply(lambda row: u'\u2705' if row['Duplicate Rows'] == 0 else u'\u274C', axis = 1)
    print(duplicate_rows_df)
    return test(len(duplicate_rows_df.loc[duplicate_rows_df.Test == u'\u274C'].index) == 0,
                'There are duplicate entries in at least one BigQuery table. Check the printed dataframe for more information.')

def statcast_regular_season_pitch_count_check():
    print('\nAccurate # of Regular Season Statcast Pitches check:')
    big_query_df = query_to_df(queries.statcast_pitches_by_year).set_index('Year').sort_index()

    baseball_savant_req = requests.get(
        f'https://baseballsavant.mlb.com/statcast_search?hfPT=&hfAB=&hfGT=R%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea={"%7C".join([str(year) for year in big_query_df.index])}%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt=&game_date_lt=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=league-year&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc#results',
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
    )
    baseball_savant_df = pd.read_html(baseball_savant_req.text)[0].set_index('Year').sort_index().loc[:, ['Pitches']]

    diff_df = baseball_savant_df.merge(big_query_df, left_index = True, right_index = True)
    diff_df.rename({'Pitches': 'Baseball Savant', 'Big Query Pitches': 'Big Query'}, axis = 1, inplace = True)
    diff_df['Test'] = diff_df.apply(lambda row: u'\u2705' if row['Big Query'] == row['Baseball Savant'] else u'\u274C', axis = 1)
    print(diff_df)
    return test(len(diff_df.loc[diff_df.Test == u'\u274C'].index) == 0,
                'The number of pitches in the "mlb.statcast_pitches" table does not align with what Baseball Savant says it should. ' + \
                    'Check the printed dataframe for more information.')
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
# ------------------------------------- Statistical Accuracy ------------------------------------- #
# ------------------------------------------------------------------------------------------------ #
def batter_season_counting_stats():
    today = date.today()
    # If April or earlier, only check stats for previous years.
    # Otherwise, include this year
    # b_hit_into_play does not pass for Matt Carpenter, Gary Sanchez & Adam Duvall in 2016, so start with 2017
    random_year = randint(2017, today.year - int(today.month < 5))
    print(f'\nRegular Season Batter Counting Stats ({random_year}) check:')

    baseball_savant_req = requests.get(
        f'https://baseballsavant.mlb.com/leaderboard/custom?year={random_year}&type=batter&min=1',
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
    )
    baseball_savant_df = pd.DataFrame(json.loads(re.search('var data = (\[.*\]);', baseball_savant_req.text)[1]))
    baseball_savant_df = baseball_savant_df \
        .astype({col: (str if col == 'player_name' else int if col == 'player_id' else float) for col in baseball_savant_df.columns})
    big_query_df = query_to_df(f'''
        SELECT
            batter player_id,
            COUNTIF(description = "hit_into_play") b_hit_into_play,
            COUNTIF(events IN ("single", "double", "triple", "home_run")) hit
        FROM
            `mlb.statcast_pitches`
        WHERE
            game_year = {random_year} AND game_type = "R"
        GROUP BY
            batter
    ''')
    test_cols = [col for col in big_query_df.columns if col != 'player_id']
    compare_df = baseball_savant_df.loc[:, ['player_id', 'player_name'] + test_cols] \
        .merge(big_query_df, how = 'outer', on = 'player_id', suffixes = ('_savant', '_bigquery')) \
            .set_index('player_id') \
                .fillna(0)
    compare_df['Diff'] = ''
    for col in test_cols:
        compare_df.Diff = compare_df.apply(lambda row: row['Diff'] if (row[f'{col}_savant'] == row[f'{col}_bigquery']) \
                                           else col if row['Diff'] == '' else ', '.join([row['Diff'], col]), axis = 1)
    compare_df = compare_df.loc[compare_df.Diff != '']
    return test(len(compare_df.index) == 0,
                'The number of pitches in the "mlb.statcast_pitches" table does not align with what Baseball Savant says it should.' + \
                    'Check the printed dataframe for more information.' + \
                        compare_df.loc[:, ['player_name', 'Diff']].to_string())
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #

def daily_tests():
    test_list = [
        duplicate_rows_check(),
        statcast_regular_season_pitch_count_check(),
        batter_season_counting_stats()
    ]
    result = f'{sum(test_list)} out of {len(test_list)} tests passed.'
    print()
    assert all(test_list), ' '.join(['Only', u'\u274C', result])
    print(u'\u2705', result)

if __name__ == '__main__':
    daily_tests()