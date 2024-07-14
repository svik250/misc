import pandas as pd
import requests
import time
import psycopg2
from sqlalchemy import create_engine

TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6IlI1NTIzNjUyMiIsInJvbGVzIjowLCJ1c2VydHlwZSI6IlVTRVIiLCJ0b2tlbiI6ImV5SmhiR2NpT2lKU1V6STFOaUlzSW5SNWNDSTZJa3BYVkNKOS5leUoxYzJWeVgzUjVjR1VpT2lKamJHbGxiblFpTENKMGIydGxibDkwZVhCbElqb2lkSEpoWkdWZllXTmpaWE56WDNSdmEyVnVJaXdpWjIxZmFXUWlPak1zSW5OdmRYSmpaU0k2SWpNaUxDSmtaWFpwWTJWZmFXUWlPaUptWXpoa01UVXdZUzFtT1RZMUxUTXhNR1l0T1RabFlTMWpZVGcwWWpka01qWmxZVElpTENKcmFXUWlPaUowY21Ga1pWOXJaWGxmZGpFaUxDSnZiVzVsYldGdVlXZGxjbWxrSWpvekxDSndjbTlrZFdOMGN5STZleUprWlcxaGRDSTZleUp6ZEdGMGRYTWlPaUpoWTNScGRtVWlmWDBzSW1semN5STZJblJ5WVdSbFgyeHZaMmx1WDNObGNuWnBZMlVpTENKemRXSWlPaUpTTlRVeU16WTFNaklpTENKbGVIQWlPakUzTWpFd05EVXlNVGdzSW01aVppSTZNVGN5TURrMU9EYzFOaXdpYVdGMElqb3hOekl3T1RVNE56VTJMQ0pxZEdraU9pSmtOelJtWTJGa01TMWhaVGN3TFRRMlpHUXRPRFpoTmkwek0yVmpaVEV3WlRjd09EWWlmUS5Cb2Z2dXNYUUtYYzN4UUR4ak02V1p5VWhwdWEwalJZOVM0YjVfV2dOLWxqZl9NZ1FnNmVnN1BUaENfc2ZGdzBmWFNLLWxiWkZ0ajNobHpORnFsVmhvUk9tR1FaMTFHQndfRm1Qd0lLNjlab1k0Z1ZxS2prTVBZX183bXFGTlZKd0FDZWpZR295MDRBN3g4T2NicVJRejF0MWdRMEg0VVpSOHNmTVR6VEZxa1EiLCJBUEktS0VZIjoidXNtUm1oWjEiLCJpYXQiOjE3MjA5NTg4MTYsImV4cCI6MTcyMTA0NTIxOH0.7VYVuz0HPyA69Z7tZU14m_YIkhIVrxh_PhHcpNeyqPWFSa7QvgwyZoxMfkVxvV_BzkW8HJY8j4C6p8w6vHmTPg"

def get_all_symbols():
    try:
        connection = psycopg2.connect(host='localhost',
                                             database='history',
                                             user='postgres',
                                             password='adminadmin',
                                             port =5432)
        sql_fetch_query = """select distinct token as id, name as symbol from nifty500""" #where token not in (select distinct token from history_candles_day)"""
        cursor = connection.cursor()
        cursor.execute(sql_fetch_query)
        idlist = cursor.fetchall()
        connection.close()
        return idlist
    except Exception as e:
        print("Error while connecting to Postgres server", e)

def get_historical_data(item, from_datetime, to_datetime, interval):
    url = "https://apiconnect.angelbroking.com/rest/secure/angelbroking/historical/v1/getCandleData"
    auth_token = 'Bearer ' + TOKEN
    payload = {
                "exchange": "NSE",
                "symboltoken": item[0],
                "interval": interval,
                "fromdate": from_datetime,
                "todate": to_datetime
                }
    headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'X-UserType': 'USER',
                'X-SourceID': 'WEB',
                'X-MACAddress': '98-48-27-CA-27-A1',
                'X-PrivateKey': 'usmRmhZ1',
                'Authorization': auth_token
                }
    historical_data_response = requests.post(url, headers=headers, json=payload)
    return historical_data_response

def get_all_historical(table, from_datetime, to_datetime, interval, mode):
    engine = create_engine('postgresql://postgres:adminadmin@localhost:5432/history')
    all_dfs = []
    all_historical = pd.DataFrame()
    idlist = get_all_symbols()
    for iditem in idlist:
        time.sleep(1)
        historical_data_response = get_historical_data(iditem, from_datetime, to_datetime, interval)
        print(iditem, historical_data_response.status_code)
        #historical_data_response_status = historical_data_response.json()["status"]
        historical_data = historical_data_response.json()["data"]
        candles = pd.DataFrame(historical_data, columns=['datetime','open','high','low','close','volume'])
        candles['token'] = iditem[0]
        candles['symbol'] = iditem[1]
        #all_dfs.append(candles)
        print(candles.shape)
        candles.to_sql(table, engine, index=False, if_exists=mode)
    #all_historical = pd.concat(all_dfs).reset_index(drop=True)
    print('all_historical load completed')

if __name__ == "__main__":
    from_datetime = "2024-06-28 09:15"
    to_datetime = "2024-07-12 15:30"
    interval = "ONE_MINUTE"
    mode = "append"
    get_all_historical('candle60', from_datetime, to_datetime, interval, mode)