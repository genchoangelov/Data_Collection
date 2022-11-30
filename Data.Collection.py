import requests, json
import logging
import time
import os
from datetime import datetime
from configparser import ConfigParser
import mysql.connector

def main():
    global ftx_url_order, kraken_url_order
    ftx_url_order = 'https://ftx.com/api/orders'
    kraken_url_order = "https://api.kraken.com"
    time_kraken = requests.get('https://api.kraken.com/0/public/Time')
    timestamp = time_kraken.json()
    timestamp = timestamp['result']['unixtime']
    timestamp = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

    binance_url = requests.get('https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT')
    binance_data = binance_url.json()
    kraken_url = requests.get('https://api.kraken.com/0/public/Ticker?pair=XBTUSDT')
    kraken_data = kraken_url.json()
    ftx_url = requests.get('https://ftx.com/api/markets/BTC/USDT')
    ftx_data = ftx_url.json()

    # calculating Differences

    global kraken_bid, ftx_ask, kraken_ask, ftx_bid, binance_ask, binance_bid, diff_K_F, diff_F_K, diff_B_F,\
        diff_F_B, diff_B_K, diff_K_B
    kraken_bid = float(kraken_data['result']['XBTUSDT']['b'][0])
    ftx_ask = float(ftx_data['result']['ask'])
    kraken_ask = float(kraken_data['result']['XBTUSDT']['a'][0])
    ftx_bid = float(ftx_data['result']['bid'])
    binance_ask = float(binance_data['askPrice'])
    binance_bid = float(binance_data['bidPrice'])

    diff_K_F = kraken_bid - ftx_ask
    diff_F_K = ftx_bid - kraken_ask
    diff_B_F = binance_bid - ftx_ask
    diff_F_B = ftx_bid - binance_ask
    diff_B_K = binance_bid - kraken_ask
    diff_K_B = kraken_bid - binance_ask

    mydb = mysql.connector.connect(
        host="localhost",
        user="username",
        password="pass",
        database="BTC_db")

    cursor = mydb.cursor()
    sql = "INSERT INTO binance_ftx_kraken (TradeDate, KrakenAskPrice, KrakenBidPrice, FtxAsk, FtxBid, BinanceAsk, BinanceBid, diff_K_F, diff_F_K, diff_B_F, diff_F_B, diff_B_K, diff_K_B) " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (timestamp, kraken_data['result']['XBTUSDT']['a'][0], kraken_data['result']['XBTUSDT']['b'][0], ftx_data['result']['ask'], ftx_data['result']['bid'],
           binance_data['askPrice'], binance_data['bidPrice'], diff_K_F, diff_F_K, diff_B_F, diff_F_B, diff_B_K, diff_K_B)
    try:
            cursor.execute(sql, val)
    except Exception:
        print("Duplicate rows")

    mydb.commit()

    print(timestamp, kraken_data['result']['XBTUSDT']['a'][0], kraken_data['result']['XBTUSDT']['b'][0], ftx_data['result']['ask'], ftx_data['result']['bid'],
    binance_data['askPrice'], binance_data['bidPrice'], diff_K_F, diff_F_K, diff_B_F, diff_F_B, diff_B_K, diff_K_B)

while True:
    main()