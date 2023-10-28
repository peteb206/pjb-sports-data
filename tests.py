from big_query_utils import queries, query_to_df
import requests
import pandas as pd
from random import randint
from datetime import date
from pybaseball import statcast_batter_expected_stats

# ------------------------------------------------------------------------------------------------ #
# ----------------------------------------- Data Quality ----------------------------------------- #
# ------------------------------------------------------------------------------------------------ #
def duplicate_rows_check():
    print('\nDuplicate Rows check:')
    duplicate_rows_df = query_to_df(queries.duplicate_rows_check).set_index('Table')
    duplicate_rows_df['Test'] = duplicate_rows_df.apply(lambda row: u'\u2705' if row['Duplicate Rows'] == 0 else u'\u274C', axis = 1)
    print(duplicate_rows_df)
    assert len(duplicate_rows_df.loc[duplicate_rows_df.Test == u'\u274C'].index) == 0, \
        'There are duplicate entries in at least one BigQuery table. Check the printed dataframe for more information.'

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
    assert len(diff_df.loc[diff_df.Test == u'\u274C'].index) == 0, \
        '''
        The number of pitches in the "mlb.statcast_pitches" table does not align with what Baseball Savant says it should.
        Check the printed dataframe for more information.
        '''
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
# ------------------------------------- Statistical Accuracy ------------------------------------- #
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #

def test():
    duplicate_rows_check()
    statcast_regular_season_pitch_count_check()

if __name__ == '__main__':
    test()