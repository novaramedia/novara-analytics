#!/usr/bin/python
# coding: utf-8

import codecs
import pandas as pd
import csv
import json
import dateutil.parser as dt
import mysql.connector as mysql
from pandas.io.json import json_normalize
from configparser import ConfigParser
from datetime import date, timedelta, datetime
from urllib.request import Request, urlopen


class Post_Reactions:
    def __init__(self):
        pd.options.mode.chained_assignment = None

    def reacts_cycle(self):
        reacts = ["LIKE", "LOVE", "WOW", "ANGRY", "SAD", "HAHA"]
        for a in reacts:
            df[a] = 0
        for a, index in enumerate(elem for elem in df["reactions"]):
            try:
                url = df["reactions"][a]
                while True:
                    try:
                        data_temp = pd.DataFrame(url["data"])
                        for react in reacts:
                            df[react][a] += data_temp[data_temp["type"] == react].count().values[0]
                        url = json.loads(urlopen(url["paging"]["next"]).read())
                    except KeyError:
                        break
            except NameError:
                pass

    def comments_cycle(self):
        df["comments_count"] = 0
        for a, index in enumerate(elem for elem in df["comments"]):
            try:
                url = df["comments"][a]
                while True:
                    try:
                        df["comments_count"][a] += pd.DataFrame(url["data"]).count().values[0]
                        url = json.loads(urlopen(url["paging"]["next"]).read())
                    except (TypeError, KeyError):
                        break
            except NameError:
                pass


def post_title():
    df['name'] = df['name'].str.replace('\| Novara Media', '')
    for index, row in enumerate(df["name"]):
        try:
            len(row)
        except TypeError:
            df["name"][index] = df["message"][index].split("\n")[0]
    for index, row in enumerate(df["description"]):
        try:
            len(row)
        except TypeError:
            df["description"][index] = ''


def post_shares():
    for index, row in enumerate(df["shares"]):
        try:
            df["shares"][index] = list(df["shares"][index].values())[0]
        except AttributeError:
            df["shares"][index] = 0


def run_insights(api_endpoint, token):
    try:
        df["reach"] = df["reach"]
    except KeyError:
        df["reach"] = 0

    for a, index in enumerate(elem for elem in df["id"]):
        impressions_json = json.loads(urlopen(api_endpoint + "v2.4/" + df["id"][a] + '/insights?fields=values&metric=post_impressions_unique&access_token=' + token).read())["data"][0]["values"][0]
        for item, value in enumerate(i for i in impressions_json.items()):
            df["reach"][a] = value[1]



class SQL_Commands:
    def __init__(self):
        global cursor, conn

        parser = ConfigParser()
        parser.read("whconfig.ini")

        wh_host = parser.get('MYSQL', 'host')
        wh_user = parser.get('MYSQL', 'user')
        wh_pass = parser.get('MYSQL', 'password')

        conn = mysql.connect(host=wh_host,
                             user=wh_user,
                             password=wh_pass,
                             database='NovaraWH')

        cursor = conn.cursor()

    def fb_posts(self, data):
        cursor.execute("""SELECT Post_ID FROM SocMedia_Facebook_Post WHERE Post_ID = '%s'""", data["id"])
        try:
            new_page = str(cursor.fetchall()[0][0])
        except IndexError:
            new_page = '0'

        if new_page == '0':
            cursor.execute("""INSERT INTO SocMedia_Facebook_Post  ( Post_ID,      Post_Type,  PostingDate, Title,
                                                                    Description,  Likes,      Love,        Sad,
                                                                    Angry,        Haha,       Wow,         Comments,
                                                                    Shares,       Impressions)
                              SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s;""",
                           [data["id"], data["type"], dt.parse(data["created_time"]), data['name'], data['description'],
                            data['LIKE'], data['LOVE'], data['SAD'], data['ANGRY'], data['HAHA'], data['WOW'], data['comments_count'], data['shares'], data['reach']])
            conn.commit()
        else:
            cursor.execute("""  UPDATE 	SocMedia_Facebook_Post s
                                SET		Likes = %s,
                                        Love = %s,
                                        Sad = %s,
                                        Angry = %s,
                                        Haha = %s,
                                        Wow = %s,
                                        Comments = %s,
                                        Shares = %s,
                                        Impressions = %s
                                WHERE Post_ID = %s;""", [data['LIKE'], data['LOVE'], data['SAD'], data['ANGRY'], data['HAHA'], data['WOW'], data['comments_count'], data['shares'], data['reach'], data["id"]])
            conn.commit()


if __name__ == '__main__':
    parser = ConfigParser()
    parser.read("apikeys.ini")
    page_id = parser.get('Facebook', 'PAGEID')
    access_token = parser.get('Facebook', 'ACCESS_TOKEN')
    api_endpoint = "https://graph.facebook.com/"

    reader = codecs.getreader("utf-8")
    fb_graph_url = api_endpoint + 'v2.12/me?fields=id%2Cname%2Cposts%7Btype%2Ccreated_time%2Cname%2Cmessage%2Creactions%2Cshares%2Ccomments%2Cdescription%7D&access_token=' + access_token
    data = json.loads(urlopen(fb_graph_url).read())

    global df
    df = pd.DataFrame.from_records(data["posts"]["data"])

    post_title()
    post_shares()
    Post_Reactions().reacts_cycle()
    Post_Reactions().comments_cycle()
    del df["comments"], df["reactions"]

    run_insights(api_endpoint=api_endpoint, token=access_token)

    sql = SQL_Commands()
    for index, row in df.iterrows():
        sql.fb_posts(data=row)
