import pandas as pd
pd.options.mode.chained_assignment = None
import datetime
import os
from sqlalchemy import create_engine
from dateutil import parser

db_host = 'localhost'
db_name = 'history'
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_port = os.getenv('DB_PORT')

engine = create_engine("postgresql+psycopg2://{user}:{pw}@{host}/{db}".format(host=db_host, db=db_name, user=db_user, pw=db_password))

def get_positive_momentum(day_candles, stag_threshold, stag_percent, rebound_percent, min_move_percent):
    start = 0
    end = 0
    stag_start = 0
    stag_count = 1
    start_time = None
    end_time = None
    big_moves = []
    for index, candle in day_candles.iterrows():
        date = candle["date"]
        symbol = candle["symbol"]
        token = candle["token"]
        open = candle["open"]
        close = candle["close"]
        half = (open+close)/2
    
        if index > 0:
            if abs(half-previous_half) < 0.001*half*stag_percent and stag_start == 0:
                stag_start = previous_half
                stag_count += 1
            elif abs(half-stag_start) < 0.001*half*stag_percent and stag_start > 0:
                stag_count += 1
            elif abs(half-stag_start) > 0.001*half*stag_percent:
                stag_start = 0
                stag_count = 1
    
        if start != 0:
            if (close-start) < 0.1*(end-start)*rebound_percent or stag_count > stag_threshold or parser.parse(candle["datetime"]).time() == datetime.time(15, 27):
                if end-start > 0.01*start*min_move_percent:
                    move_df = day_candles[(day_candles['datetime'] >= start_time) & (day_candles['datetime'] <= end_time)]
                    move_df['change'] = round(abs(move_df['close'] - move_df['open']), 2)
                    max_change = move_df['change'].max()
                    min_change = move_df['change'].min()
                    average_change = round(move_df['change'].mean(),2)
                    max_volume = move_df['volume'].max()
                    min_volume = move_df['volume'].min()
                    average_volume = round(move_df['volume'].mean(),2)
                    total_candles = len(move_df)
                    big_moves.append([date, symbol, token, start_time, end_time, start, end, round(100*(end-start)/start,2), max_change, min_change, average_change, max_volume, min_volume, average_volume, total_candles, 'positive'])
                start = 0
                end = 0
                start_time = None
                end_time = None
            
            elif (close-start) > 0.1*(end-start)*rebound_percent:
                if close > end:
                    end = close
                    end_time = candle["datetime"]
    
        elif start == 0 and stag_count <= stag_threshold:
            if close > open:
                start =  open
                end = close
                start_time = candle["datetime"]
                end_time = candle["datetime"]
    
        previous_half = half
    return big_moves

def get_negative_momentum(day_candles, stag_threshold, stag_percent, rebound_percent, min_move_percent):
    start = 0
    end = 0
    stag_start = 0
    stag_count = 1
    start_time = None
    end_time = None
    big_moves = []
    for index, candle in day_candles.iterrows():
        date = candle["date"]
        symbol = candle["symbol"]
        token = candle["token"]
        open = candle["open"]
        close = candle["close"]
        half = (open+close)/2
    
        if index > 0:
            if abs(half-previous_half) < 0.001*half*stag_percent and stag_start == 0:
                stag_start = previous_half
                stag_count += 1
            elif abs(half-stag_start) < 0.001*half*stag_percent and stag_start > 0:
                stag_count += 1
            elif abs(half-stag_start) > 0.001*half*stag_percent:
                stag_start = 0
                stag_count = 1
    
        if start != 0:
            if (start-close) < 0.1*(start-end)*rebound_percent or stag_count > stag_threshold or parser.parse(candle["datetime"]).time() == datetime.time(15, 27):
                if start-end > 0.01*start*min_move_percent:
                    move_df = day_candles[(day_candles['datetime'] >= start_time) & (day_candles['datetime'] <= end_time)]
                    move_df['change'] = round(abs(move_df['close'] - move_df['open']), 2)
                    max_change = move_df['change'].max()
                    min_change = move_df['change'].min()
                    average_change = round(move_df['change'].mean(),2)
                    max_volume = move_df['volume'].max()
                    min_volume = move_df['volume'].min()
                    average_volume = round(move_df['volume'].mean(),2)
                    total_candles = len(move_df)
                    big_moves.append([date, symbol, token, start_time, end_time, start, end, round(100*(end-start)/start,2), max_change, min_change, average_change, max_volume, min_volume, average_volume, total_candles, 'negative'])
                start = 0
                end = 0
                start_time = None
                end_time = None
            
            elif (start-close) > 0.1*(start-end)*rebound_percent:
                if close < end:
                    end = close
                    end_time = candle["datetime"]
    
        elif start == 0 and stag_count <= stag_threshold:
            if close < open:
                start =  open
                end = close
                start_time = candle["datetime"]
                end_time = candle["datetime"]
    
        previous_half = half
    return big_moves

def get_momentum(day_candles, stag_threshold=8, stag_percent=3, rebound_percent=6.5, min_move_percent=1.8):
    positive_moves = get_positive_momentum(day_candles, stag_threshold, stag_percent, rebound_percent, min_move_percent)
    negative_moves = get_negative_momentum(day_candles, stag_threshold, stag_percent, rebound_percent, min_move_percent)
    all_moves = positive_moves + negative_moves
    return all_moves

unique_instances = pd.read_sql_query("select distinct token, symbol, date(datetime) as date from candle120 order by symbol, date(datetime)", con=engine)
all_moves = []
for index, instance in unique_instances.iterrows():
    date = instance['date']
    symbol = instance['symbol']
    token = instance['token']
    print(f"Processing {symbol} on {date}")
    day_candles = pd.read_sql_query(f"select * from candle120 where token = '{token}' and symbol = '{symbol}' and date(datetime) = '{date}' order by datetime", con=engine)
    day_candles['date'] = day_candles['datetime'].dt.date
    day_candles['datetime'] = day_candles['datetime'].astype(str)
    if len(day_candles) > 0:
        all_moves += get_momentum(day_candles)

momentum_df = pd.DataFrame(all_moves, columns=['date', 'symbol', 'token', 'start_time', 'end_time', 'start', 'end', 'percent','max_change', 'min_change', 'average_change', 'max_volume', 'min_volume', 'average_volume', 'total_candles', 'direction'])
momentum_df.to_sql('momentum', con=engine, if_exists='replace', index=False)