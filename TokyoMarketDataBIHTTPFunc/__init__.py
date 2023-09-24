import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import logging
import os

import azure.functions as func
from azure.data.tables import TableServiceClient

'''
SUMMARY
----------
HTTPS通信でデータ取得を行う

PARAMETERS
----------
@QUERY: date

RETURNS
----------
STATUS: 200
'''
def main(req: func.HttpRequest) -> func.HttpResponse:
    date = req.params.get('date')

    # データを取得する
    rows = get_data(date)

    # データを整形する
    df = format_data(rows)

    # データを保存する
    save_data(df, date)

    return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
    )

'''
SUMMARY
--------
東京都の卸売市場のデータを取得する

PARAMETERS
----------
None

RETURNS
-------
rows : list
'''
def get_data(date):
    #  東京卸売市場のデータをスクレイピングする
    baseurl = "https://www.shijou.metro.tokyo.lg.jp/torihiki/03/suisan/yyyyMMn.html"
    url = baseurl.replace("yyyyMMn", date)

    # データを取得する
    res = requests.get(url)
    soup = BeautifulSoup(res.content, "html.parser")
    # テーブルを取得する
    table = soup.find_all("table")
    # テーブルの行を取得する
    rows = table[3].findAll("tr")

    return rows

'''
SUMMARY
--------
データを整形する

PARAMETERS
----------
rows : list

RETURNS
-------
df : DataFrame
'''
def format_data(rows):
    # データを取得する
    data = []

    # rowspan="2"の場合前の行のデータを取得する
    tempcol = []
    maxRowCount = 0
    rowcount = 1

    for row in rows[2:]:
        cols = row.findAll("td")

        # tempcolがある場合はデータを取得する
        if int(rowcount) <= int(maxRowCount):
            cols = tempcol + cols
            cols = [v.text.replace("\n", "") for v in cols]
            data.append(cols)
            rowcount += 1
            if int(rowcount) == int(maxRowCount):
                rowcount = 1
                maxRowCount = 0
                tempcol = []
            continue

        # rowspan="2"の場合
        maxRowCount = cols[0].attrs.get("rowspan")
        if maxRowCount is None:
            maxRowCount = 0
            continue

        if 1 < int(maxRowCount):
            tempcol = row.findAll("td", {"rowspan": maxRowCount})

            # rowspan=2以外のデータを取得する
            cols = [v.text.replace("\n", "") for v in cols]
            data.append(cols)
            continue

        cols = [v.text.replace("\n", "") for v in cols]
        data.append(cols)
        maxRowCount = 0

    # データフレームを作成する
    df = pd.DataFrame(data)

    return df

'''
SUMMARY
--------
データを保存する

PARAMETERS
----------
df : DataFrame

RETURNS
-------
None
'''
def save_data(df, date):
    service = TableServiceClient.from_connection_string(conn_str=os.environ.get("AzureWebJobsStorage"))
    table_client = service.get_table_client(table_name="CentralWholesaleMarketTable")

    # データを登録する
    for index, row in df.iterrows():
        entity = {
            "PartitionKey": date,
            "RowKey": str(index),
            "ItemName": row[0],
            "Quantity": row[1],
            "WeeklyRatio": row[2],
            "WeeklyIncreaseDecrease": row[3],
            "LastYearRatio": row[4],
            "LastYearIncreaseDecrease": row[5],
            "Birthplace": row[6],
            "Brand": row[7],
            "HighPrice": row[8],
            "MiddlePrice": row[9],
            "LowPrice": row[10],
            "WeeklyRatio": row[11],
            "Size": row[12]
        }
        table_client.create_entity(entity=entity)
