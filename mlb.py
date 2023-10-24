import big_query_utils
from pybaseball import statcast
from datetime import date
import time

def add_statcast_data_to_big_query(game_date: date) -> bool:
    '''
    Fetch a given day's Statcast data from Baseball Savant and add it to the "PJB Sports Data" BigQuery project

    Return: a boolean value indicating whether new data was added to BigQuery (True) or not (False)
    '''
    game_date_str = game_date.strftime('%Y-%m-%d')
    data_fetch_start_time = time.perf_counter()
    pitches_df = statcast(start_dt = game_date_str, end_dt = game_date_str)
    data_fetch_end_time = time.perf_counter()
    print(f'Downloaded the statcast data for {game_date_str} in {data_fetch_end_time - data_fetch_start_time:0.2f} seconds')
    if len(pitches_df.index) > 0:
        games_df = pitches_df.loc[:, statcast_game_columns].drop_duplicates('game_pk')
        if len(games_df.index) > 0:
            big_query_utils.add_df_rows_to_table('mlb.statcast_games', games_df)

        at_bats_df = pitches_df \
            .sort_values(['game_pk', 'at_bat_number', 'pitch_number']) \
                .loc[:, statcast_at_bat_columns] \
                    .drop_duplicates(['game_pk', 'at_bat_number'], keep = 'last')
        if len(at_bats_df.index) > 0:
            big_query_utils.add_df_rows_to_table('mlb.statcast_at_bats', at_bats_df)

        pitches_df.drop(
            [col for col in statcast_game_columns + statcast_at_bat_columns + statcast_drop_columns if col not in ['game_pk', 'at_bat_number']],
            axis = 1, inplace = True
        )
        big_query_utils.add_df_rows_to_table('mlb.statcast_pitches', pitches_df)
        print(f'Added the statcast data for {game_date_str} to BigQuery in {time.perf_counter() - data_fetch_end_time:0.2f} seconds')
        return True
    return False

class queries:
    existing_game_dates = '''
        SELECT
            DISTINCT game_date
        FROM
            `mlb.statcast_games`
    '''

    duplicate_rows = ''
    for table_name, keys in {
        'mlb.statcast_games': ['game_pk'],
        'mlb.statcast_at_bats': ['game_pk', 'at_bat_number'],
        'mlb.statcast_pitches': ['game_pk', 'at_bat_number', 'pitch_number']
    }.items():
        duplicate_rows += f'''
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
            UNION ALL
        '''
    duplicate_rows = duplicate_rows[:-25] # Drop last UNION ALL

    def full_statcast_data(start_date: date, end_date: date):
        return f'''
            SELECT
                *
            FROM
                `mlb.statcast_games` games
            JOIN
                `mlb.statcast_at_bats` at_bats
            USING
                (game_pk)
            JOIN
                `mlb.statcast_pitches` pitches
            USING
                (game_pk, at_bat_number)
            WHERE
                game_date >= TIMESTAMP("{start_date.strftime('%Y-%m-%d')}") AND
                game_date <= TIMESTAMP("{end_date.strftime('%Y-%m-%d')}")
        '''

statcast_drop_columns = ['spin_dir', 'spin_rate_deprecated', 'break_angle_deprecated', 'break_length_deprecated', 'tfs_deprecated',
                         'tfs_zulu_deprecated', 'umpire', 'sv_id', 'pitcher.1', 'fielder_2.1']

statcast_game_columns = ['game_pk', 'game_year', 'game_type', 'game_date', 'away_team', 'home_team']

statcast_at_bat_columns = ['game_pk', 'at_bat_number', 'inning', 'inning_topbot', 'events', 'des', 'hit_location', 'bb_type', 'hc_x', 'hc_y',
                           'estimated_ba_using_speedangle', 'estimated_woba_using_speedangle', 'woba_value', 'woba_denom', 'babip_value',
                           'iso_value', 'launch_speed_angle', 'fld_score', 'post_fld_score']