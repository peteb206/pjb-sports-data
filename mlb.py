from big_query_utils import add_df_rows_to_table, query_to_df, queries
from pybaseball import statcast
from datetime import datetime, date
import time
import requests
import json
import pandas as pd

def add_statcast_data_to_big_query(game_date: date):
    '''
    Fetch a given day's Statcast data from Baseball Savant and add it to the "PJB Sports Data" BigQuery project
    '''
    game_date_str = game_date.strftime('%Y-%m-%d')
    data_fetch_start_time = time.perf_counter()
    pitches_df = statcast(start_dt = game_date_str, end_dt = game_date_str)
    data_fetch_end_time = time.perf_counter()
    print(f'Downloaded the statcast data for {game_date_str} in {data_fetch_end_time - data_fetch_start_time:0.2f} seconds')
    if (len(pitches_df.index) > 0) & ('game_date' in pitches_df.columns):
        pitches_df.game_date = pitches_df.game_date.apply(lambda x: x.date())
        add_df_rows_to_table('mlb.statcast_pitches', pitches_df)
        print(f'Added the statcast data for {game_date_str} to BigQuery in {time.perf_counter() - data_fetch_end_time:0.2f} seconds')

def update_injury_data_in_big_query():
    '''
    Fetch a given year's injury data from Fangraphs and add it to the "PJB Sports Data" BigQuery project
    '''
    data_fetch_start_time = time.perf_counter()
    fangraphs_base_url = 'https://www.fangraphs.com/api/roster-resource/injury-report'
    fangraphs_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36',
        'Referer': 'https://www.fangraphs.com/roster-resource/injury-report'
    }
    injuries_df = pd.DataFrame()
    for year in range(2020, date.today().year + 1):
        if year > 2020:
            time.sleep(3)
        load_date_req = requests.get(f'{fangraphs_base_url}/loaddate', params = {'season': year}, headers = fangraphs_headers)
        injuries_req = requests.get(f'{fangraphs_base_url}/data', params = {'loaddate': load_date_req.text.replace('"', ''), 'season': year},
                                    headers = fangraphs_headers)
        injuries_df = pd.concat([injuries_df, pd.DataFrame(json.loads(injuries_req.text))], ignore_index = True)
    injuries_df = injuries_df.loc[~injuries_df.season.isna()].astype({'season': int})
    injuries_df.loaddate = pd.to_datetime(injuries_df.loaddate)
    for col in ['date', 'retrodate', 'eligibledate', 'returndate']:
        injuries_df[col] = injuries_df[col].apply(lambda x: datetime.strptime(x, '%m/%d/%y').date() if x not in ['', None] else None)
    query_to_df(queries.clear_fangraphs_injuries)
    add_df_rows_to_table('mlb.fangraphs_injuries', injuries_df)
    print(f'Updated the injury data in BigQuery in {time.perf_counter() - data_fetch_start_time:0.2f} seconds')