import time
import datetime
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import logging
import os

# Configure logging to capture INFO level messages and above
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a logger instance
logger = logging.getLogger(__name__)

db_host = 'localhost'
db_name = 'history'
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_port = os.getenv('DB_PORT')

class CandleMaker:
    def __init__(self, engine):
        self.engine = engine
        self.cursor = None
        self.candle_last_timestamp = {'candle120': None, 'candle180': None}
        self.first_tick = {'candle120': None, 'candle180': None}
        self.first_candle = {'candle120': None, 'candle180': None}
        self.CURRENT_DATE = None
    
    def get_cursor(self):
        try:
            connection = psycopg2.connect(host=db_host,
                                            database=db_name,
                                            user=db_user,
                                            password=db_password,
                                            port=db_port)
            cursor = connection.cursor()
            return cursor
        except psycopg2.OperationalError as e:
            print(f"Error while connecting to Postgres: {e}")

    def get_ticks_last_timestamp(self):
        cursor = self.cursor
        cursor.execute(f"""select max(datetime) from candle60 where cast(datetime as time) >= '9:15:00' and cast(datetime as time) <= '15:30:00' and cast(datetime as date) = '{self.CURRENT_DATE}'""")
        last_timestamp = cursor.fetchone()[0]
        return last_timestamp
    
    def first_stream(self, table):
        if self.first_tick[table] is not None:
            return True
        else:
            cursor = self.cursor
            cursor.execute(f"""select max(datetime) from candle60 where cast(datetime as time) >= '9:15:00' and cast(datetime as time) <= '15:30:00' and cast(datetime as date) = '{self.CURRENT_DATE}'""")
            last_timestamp = cursor.fetchone()[0]
            if last_timestamp is None:
                return False
            else:
                self.first_tick[table] = last_timestamp
                return True
    
    def get_ticks_first_timestamp(self, candle_last_timestamp=None):
        if candle_last_timestamp is None:
            cursor = self.cursor
            cursor.execute(f"""select min(datetime) from candle60 where cast(datetime as time) >= '9:15:00' and cast(datetime as date) = '{self.CURRENT_DATE}'""")
            first_timestamp = cursor.fetchone()[0]
            return first_timestamp
        else:
            cursor = self.cursor
            cursor.execute(f"""select min(datetime) from candle60 where datetime >= '{candle_last_timestamp}' and cast(datetime as time) >= '9:15:00' and cast(datetime as date) = '{self.CURRENT_DATE}'""")
            first_timestamp = cursor.fetchone()[0]
            return first_timestamp

    def get_candle_last_timestamp(self, table):
        cursor = self.cursor
        cursor.execute(f"""select max(datetime) from {table} where cast(datetime as date) = '{self.CURRENT_DATE}'""")
        last_timestamp = cursor.fetchone()[0]
        return last_timestamp
    
    def first_load(self, table):
        if self.first_candle[table] is not None:
            return True
        else:
            cursor = self.cursor
            cursor.execute(f"""select max(datetime) from {table} where cast(datetime as date) = '{self.CURRENT_DATE}'""")
            last_timestamp = cursor.fetchone()[0]
            if last_timestamp is None:
                return False
            else:
                self.first_candle[table] = last_timestamp
                return True
    
    def get_cutoff(self, dt, period):
        table = 'candle' + str(period)
        last_timestamp = self.get_candle_last_timestamp(table)
        if dt.time() < (datetime.datetime(2022, 12, 28, 9, 15, 0) + datetime.timedelta(seconds=period)).time():
            dt = datetime.datetime(dt.year, dt.month, dt.day, 9, 15, 0)
            if last_timestamp is not None:
                if dt <= last_timestamp:
                    dt = last_timestamp
                    seconds = dt.second
                    remainder = seconds % period
                    dt += datetime.timedelta(seconds=(period - remainder))
                    return dt
                else:
                    return dt
            else:
                return dt
        else:
            seconds = dt.second
            remainder = seconds % period
            dt += datetime.timedelta(seconds=(period - remainder))
            return dt

    def get_candle(self, cutoff_time, duration):
        if (cutoff_time + datetime.timedelta(seconds=duration)).time() >= datetime.time(15, 30, 0):
            end_time = datetime.datetime(cutoff_time.year, cutoff_time.month, cutoff_time.day, 15, 30, 1)
        else:
            end_time = cutoff_time + datetime.timedelta(seconds=duration)
        candle_query = f"""with duration_ticks as
        (
        select token, symbol, datetime, open, high, low, close, volume,
        row_number() over (partition by token order by datetime) as inc_row_num,
        row_number() over (partition by token order by datetime desc) as dec_row_num from
        (
        select token, symbol, datetime, open, high, low, close, volume from candle60
        where datetime >= '{cutoff_time}'
        and datetime < '{end_time}'
        ) tticks
        )
        select s.token as token, s.name as symbol, '{cutoff_time}' as datetime, ot.open, ht.high, lt.low, ct.close, vt.volume from nifty500 s 
        inner join
        (select token, open from duration_ticks where inc_row_num = 1) ot
        on s.token=ot.token
        inner join
        (select token, max(high) as high from duration_ticks group by token) ht
        on s.token=ht.token
        inner join
        (select token, min(low) as low from duration_ticks group by token) lt
        on s.token=lt.token
        inner join
        (select token, close from duration_ticks where dec_row_num = 1) ct
        on s.token=ct.token
        inner join
        (select token, sum(volume) as volume from duration_ticks group by token) vt
        on s.token=vt.token;"""
        cursor = self.cursor
        cursor.execute(candle_query)
        ticks = cursor.fetchall()
        return pd.DataFrame(ticks, columns=['token', 'symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume'])
    
    def get_tick(self, period, ticks_last_timestamp):
        table = 'candle' + str(period)
        if self.candle_last_timestamp[table] is not None and self.first_load(table) is not None:
            if datetime.datetime.now() <= (self.candle_last_timestamp[table] + datetime.timedelta(seconds=2*period)) and datetime.datetime.now().time() <= datetime.time(15, 30, 0):
                print(f"First block if - Waiting for new ticks for {table} at {datetime.datetime.now()}")

            elif datetime.datetime.now() >= (self.candle_last_timestamp[table] + datetime.timedelta(seconds=2*period)) \
            and ticks_last_timestamp >= (self.candle_last_timestamp[table] + datetime.timedelta(seconds=2*period)):
                cutoff_time = self.get_cutoff(self.get_ticks_first_timestamp(self.candle_last_timestamp[table]), period)
                ticks_df = self.get_candle(cutoff_time, period)
                ticks_df.to_sql(table, self.engine, if_exists='append', index=False)
                print(f"First Block - candle_last_timestamp: {self.candle_last_timestamp[table]} and ticks_last_timestamp: {ticks_last_timestamp} for period {period}")
                print(f"First Block - Candles written for time {cutoff_time} for {table} at {datetime.datetime.now()}")
                self.candle_last_timestamp[table] = cutoff_time

            elif (self.candle_last_timestamp[table] + datetime.timedelta(seconds=2*period)).time() >= datetime.time(15, 30, 0) and ticks_last_timestamp > self.candle_last_timestamp[table] + datetime.timedelta(seconds=period):
                cutoff_time = self.get_cutoff(self.get_ticks_first_timestamp(self.candle_last_timestamp[table]), period)
                ticks_df = self.get_candle(cutoff_time, period)
                ticks_df.to_sql(table, self.engine, if_exists='append', index=False)
                print(f"First Block 330- candle_last_timestamp: {self.candle_last_timestamp[table]} and ticks_last_timestamp: {ticks_last_timestamp} for period {period}")
                print(f"First Block 330- Candles written for time {cutoff_time} for {table} at {datetime.datetime.now()}")
                self.candle_last_timestamp[table] = cutoff_time

            else:
                return True
        
        else:
            self.candle_last_timestamp[table] = self.get_candle_last_timestamp(table)
            if self.candle_last_timestamp[table] is None:
                self.candle_last_timestamp[table] = self.get_cutoff(self.get_ticks_first_timestamp(), period)

            if self.candle_last_timestamp[table] is not None:
                if self.candle_last_timestamp[table].time() == datetime.time(9, 15, 0):
                    if datetime.datetime.now() <= (self.candle_last_timestamp[table] + datetime.timedelta(seconds=period)):
                        print(f"Second block if - Waiting for new ticks for {table} at {datetime.datetime.now()}")

                    elif datetime.datetime.now() >= (self.candle_last_timestamp[table] + datetime.timedelta(seconds=period)) \
                    and ticks_last_timestamp >= (self.candle_last_timestamp[table] + datetime.timedelta(seconds=period)):
                        cutoff_time = self.get_cutoff(self.get_ticks_first_timestamp(self.candle_last_timestamp[table]), period)
                        ticks_df = self.get_candle(cutoff_time, period)
                        ticks_df.to_sql(table, self.engine, if_exists='append', index=False)
                        print(f"Second Block - candle_last_timestamp: {self.candle_last_timestamp[table]} and ticks_last_timestamp: {ticks_last_timestamp} for period {period}")
                        print(f"Second Block - Candles written for time {cutoff_time} for {table} at {datetime.datetime.now()}")
                        self.candle_last_timestamp[table] = cutoff_time

                else:
                    if datetime.datetime.now() <= (self.candle_last_timestamp[table] + datetime.timedelta(seconds=2*period)):
                        print(f"Second block if2 - Waiting for new ticks for {table} at {datetime.datetime.now()}")

                    elif datetime.datetime.now() >= (self.candle_last_timestamp[table] + datetime.timedelta(seconds=2*period)) \
                    and ticks_last_timestamp >= (self.candle_last_timestamp[table] + datetime.timedelta(seconds=2*period)):
                        cutoff_time = self.get_cutoff(self.get_ticks_first_timestamp(self.candle_last_timestamp[table]), period)
                        ticks_df = self.get_candle(cutoff_time, period)
                        ticks_df.to_sql(table, self.engine, if_exists='append', index=False)
                        print(f"Second Block if2 - candle_last_timestamp: {self.candle_last_timestamp[table]} and ticks_last_timestamp: {ticks_last_timestamp} for period {period}")
                        print(f"Second Block if2 - Candles written for time {cutoff_time} for {table} at {datetime.datetime.now()}")
                        self.candle_last_timestamp[table] = cutoff_time

            else:
                self.candle_last_timestamp[table] = self.get_candle_last_timestamp(table)
                print(f"Second block else - Waiting for new ticks for {table} at {datetime.datetime.now()}")

    def make_candle(self):
        while True:
            ticks_last_timestamp = self.get_ticks_last_timestamp()
            if ticks_last_timestamp is not None:
                flag = self.get_tick(120, ticks_last_timestamp)
                self.get_tick(180, ticks_last_timestamp)
                if flag:
                    break
            else:
                print(f"Main block - Waiting for new ticks at {datetime.datetime.now()}")
                break

def run_candle_maker():
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    candle_maker = CandleMaker(engine)
    candle_maker.cursor = candle_maker.get_cursor()

    date_list = pd.read_sql("select distinct cast(datetime as date) as date from temp_candle60 order by cast(datetime as date)", engine)

    for date in date_list['date']:
        next_date = date + datetime.timedelta(days=4)
        current_df = pd.read_sql(f"select * from temp_candle60 where cast(datetime as date) >= '{date}' and cast(datetime as date) <= '{next_date}'", engine)
        print(f"Making candles for {date}")
        candle_maker.cursor.execute(f"truncate table candle60")
        candle_maker.cursor.connection.commit()
        current_df.to_sql('candle60', engine, if_exists='append', index=False)
        candle_maker.CURRENT_DATE = date
        candle_maker.make_candle()
        print(f"Candles made for {date}")

if __name__ == "__main__":
    run_candle_maker()