import pandas as pd
import datetime
import os
from dateutil import parser
from sqlalchemy import create_engine
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

db_host = 'localhost'
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_port = os.getenv('DB_PORT')

engine = create_engine("postgresql+psycopg2://{user}:{pw}@{host}/{db}".format(host=db_host, db=db_name, user=db_user, pw=db_password))
interval = 60

def find_initial_positive_trend(row,initial_cutoff_percent = 0.4):
    if row['change'] > row['open']*initial_cutoff_percent/100:
        return [True, row['datetime'], None, row['close_previous'], row['close_current'], row['change_percentage'], row['volume'], 1, 0]
    else:
        return [False, None, None, None, None, None, None, 0, 1]
    
def find_positive_trend(row, retracement_threshold = 0.7, stagnation_percent = 0.3, stag_threshold = 10):   

    half = row['half']
    new_half = round((row['open'] + row['close'])/2,2)

    if abs(new_half-half) > half*stagnation_percent/100:
        stag_count = 0
        row['half'] = new_half
    else:
        stag_count = row['stag_count'] + 1

    if row['trend'] == True:
        current_change_percentage = round(((row['close']-row['start_price'])/row['start_price'])*100,2)
        trend_change = row['trend_change']
        new_volume = row['trend_volume'] + row['volume']
        if current_change_percentage <= retracement_threshold*trend_change or row['datetime_current'].time() == datetime.time(15, 29, 0) or stag_count > stag_threshold:
            return[False, False, None, None, None, None, None, None, 0, stag_count, row['half']]
        elif current_change_percentage > trend_change:
            return[False, True, row["start_datetime"], row["datetime_current"], row["start_price"], row["close"], current_change_percentage, new_volume, row["trend_candle_count"]+1, stag_count, row['half']]
        elif current_change_percentage > retracement_threshold*trend_change and stag_count <= stag_threshold:
            return[True, True, row["start_datetime"], row["end_datetime"], row["start_price"], row["end_price"], trend_change, row["trend_volume"], row["trend_candle_count"], stag_count, row['half']]
        
    else:
        if row['change'] > row['open']*stagnation_percent/100:
            row['half'] = new_half
            return [False, True, row['datetime_current'], row['datetime_current'], row['open'], row['close'], row['change_percentage'], row['volume'], 1, 0, row['half']]
        else:
            return [False, False, None, None, None, None, None, None, 0, stag_count, row['half']]

day_candles = pd.read_sql_query(f"select * from candle60 where date(datetime) = '2024-07-12' order by datetime", con=engine)
previous_day_candles = pd.read_sql_query(f"select * from candle60 where date(datetime) = '2024-07-11' order by datetime", con=engine)

idx = previous_day_candles.groupby('symbol')['datetime'].idxmax()
max_datetime_per_symbol = previous_day_candles.loc[idx]
previous_candles = max_datetime_per_symbol[["token", "symbol", "close"]].reset_index(drop=True)

distinct_datetimes = day_candles['datetime'].unique()

for distinct_datetime in distinct_datetimes:
    current_candles = day_candles[day_candles['datetime'] == distinct_datetime].reset_index(drop=True)

    if parser.parse(str(distinct_datetime)).time() == datetime.time(9, 15, 0):
        trend_summary = pd.merge(current_candles, previous_candles, how = 'inner', on=['token','symbol'], suffixes=('_current', '_previous'))
        trend_summary['change'] = trend_summary['close_current'] - trend_summary['close_previous']
        trend_summary['change_percentage'] = round((trend_summary['change']/trend_summary['close_previous'])*100,2)
        trend_summary['half'] = round((trend_summary['open'] + trend_summary['close_current'])/2,2)
        trend_summary['pause'] = False
        trend_summary['trend_props'] = trend_summary.apply(find_initial_positive_trend, axis=1)
        trend_summary[['trend', 'start_datetime', 'end_datetime', 'start_price', 'end_price', 'trend_change', 'trend_volume', 'trend_candle_count', 'stag_count']] = pd.DataFrame(trend_summary['trend_props'].to_list(), index=trend_summary.index)
        trend_summary = trend_summary.drop(columns=['open', 'high', 'low', 'volume', 'trend_props', 'change'])
        trend_summary.rename(columns={'close_current': 'previous_close', 'close_previous':'day_open', 'change_percentage':'day_change'}, inplace=True)
        trend_summary['empty'] = False

    else:
        trend_summary_copy = trend_summary.copy()
        trend_summary = pd.merge(trend_summary, current_candles, how = 'inner', on=['token','symbol'], suffixes=('_previous','_current'))
        trend_summary['change'] = trend_summary['close'] - trend_summary['previous_close']
        trend_summary['change_percentage'] = round((trend_summary['change'])*100/trend_summary['previous_close'],2)
        trend_summary['net_change'] = trend_summary['close'] - trend_summary['day_open']
        trend_summary['day_change'] = round((trend_summary['net_change']/trend_summary['day_open'])*100,2)
        trend_summary['trend_props'] = trend_summary.apply(find_positive_trend, axis=1)
        trend_summary[['pause', 'trend', 'start_datetime', 'end_datetime', 'start_price', 'end_price', 'trend_change', 'trend_volume', 'trend_candle_count', 'stag_count', 'half']] = pd.DataFrame(trend_summary['trend_props'].to_list(), index=trend_summary.index)
        trend_summary = trend_summary.drop(columns=['open', 'high', 'low', 'volume', 'trend_props', 'net_change','change_percentage','previous_close','datetime_previous','change'])
        trend_summary.rename(columns={'close': 'previous_close', 'datetime_current': 'datetime'}, inplace=True)
        trend_summary['empty'] = False

        trend_summary_empty = pd.merge(trend_summary_copy, current_candles, how = 'left', on=['token','symbol'], suffixes=('_current', '_previous'))
        trend_summary_empty = trend_summary_empty[trend_summary_empty['open'].isnull()]
        trend_summary_empty['datetime'] = trend_summary_empty['datetime_current'] + datetime.timedelta(seconds=interval)
        trend_summary_empty = trend_summary_empty.drop(columns=['open', 'high', 'close', 'low', 'volume', 'datetime_previous', 'datetime_current'])
        trend_summary_empty['empty'] = True
        trend_summary = pd.concat([trend_summary, trend_summary_empty], ignore_index=True, sort=True)
        trend_summary['type'] = 'positive'



    trend_summary.to_sql('trend_summary', engine, if_exists='append', index=False)