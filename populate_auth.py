import requests
import psycopg2
import json
import time

def update_token(token, token_type, app):
    conn = psycopg2.connect(
        host="localhost",
        database="liveangel",
        user="postgres",
        password="adminadmin"
    )
    cursor = conn.cursor()
    cursor.execute(f"UPDATE credentials set token = '{token}', updatetime = CURRENT_TIMESTAMP where type = '{token_type}' and app = '{app}'")
    conn.commit()
    cursor.close()

totp = input("Enter your TOTP: ")

url = 'https://apiconnect.angelbroking.com/rest/auth/angelbroking/user/v1/loginByPassword'

payload = "{\n\"clientcode\":\"R55236522\",\n\"password\":\"2008\"\n,\n\"totp\":\""+totp+"\"\n}"
feed_headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'X-UserType': 'USER',
    'X-SourceID': 'WEB',
    'X-MACAddress': '98-48-27-CA-27-A1',
    'X-PrivateKey': 'qExV03Qs'
  }

feed_res = requests.post(url, headers=feed_headers, data=payload)
print(f"status of feed token request: {feed_res.status_code}")

feed_jwt_token = feed_res.json()['data']['jwtToken']
feed_token = feed_res.json()['data']['feedToken']

histroy_headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'X-UserType': 'USER',
    'X-SourceID': 'WEB',
    'X-MACAddress': '98-48-27-CA-27-A1',
    'X-PrivateKey': 'usmRmhZ1'
  }

time.sleep(5)

history_res = requests.post(url, headers=histroy_headers, data=payload)
print(f"status of history token request: {history_res.status_code}")
history_jwt_token = history_res.json()['data']['jwtToken']

update_token(feed_jwt_token, 'auth', 'live')
print("live jwt token updated")

update_token(feed_token, 'feed', 'live')
print("live feed token updated")

update_token(history_jwt_token, 'auth', 'history')
print("history auth token updated")

time.sleep(10)