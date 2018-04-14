#!/usr/bin/python
# coding: utf-8

import codecs
import pandas as pd
import json
import mysql.connector as mysql
from configparser import ConfigParser
from datetime import date, timedelta, datetime
from urllib.request import urlopen
from instagram.client import InstagramAPI as insta

class InstagramFrame:
    def __init__(self):
        pd.options.mode.chained_assignment = None

    def init_data(self, data):

        i.tagged_in_photo(data=data)
        i.get_id(data=data)
        i.gen_title(data=data)
        i.date_format(data=data)
        i.comments_count(data=data)
        i.likes_count(data=data)
        i.get_dimensions(data=data)
        i.remove_tag_squarebrackets(data=data)
        i.delete_columns(data=data)

    def get_id(self, data):
        data["id"] = ''
        for a, index in enumerate(elem for elem in data["caption"]):
            try:
                data["id"][a] = index["id"]
            except TypeError:
                data["id"][a] = ''

    def gen_title(self, data):
        data["title"] = ''
        for a, index in enumerate(elem for elem in data["caption"]):
            try:
                data["title"][a] = str(index["text"]).replace("'","")
            except TypeError:
                data["title"][a] = '<NO CAPTION>'

    def comments_count(self, data):
        data["comments_count"] = 0
        for a, index in enumerate(elem for elem in data["comments"]):
            for comment in enumerate(elem for elem in index.values()):
                data["comments_count"][a] += comment[1]

    def tagged_in_photo(self, data):
        data["phototags"] = ''
        for a, index in enumerate(elem for elem in data["users_in_photo"]):
            for name in enumerate(elem for elem in index):
                data['phototags'][a] += name[1]["user"]["full_name"]+'; '

    def likes_count(self, data):
        data["likes_count"] = 0
        for a, index in enumerate(elem for elem in data["likes"]):
            for like in enumerate(elem for elem in index.values()):
                data["likes_count"][a] += like[1]

    def remove_tag_squarebrackets(self, data):
        for a, index in enumerate(elem for elem in data["tags"]):
            data["tags"][a] = str(data["tags"][a]).replace("[", "")
            data["tags"][a] = str(data["tags"][a]).replace("]", "")
            data["tags"][a] = str(data["tags"][a]).replace("'", "")

    def get_dimensions(self, data):
        data["width"] = ''
        data["height"] = ''
        for a, index in enumerate(elem for elem in data["images"]):
            data["width"][a] = index["standard_resolution"]["width"]
            data["height"][a] = index["standard_resolution"]["height"]

    def date_format(self, data):
        data["datetime"] = datetime.today().date()
        for a, index in enumerate(elem for elem in data["created_time"]):
            data["datetime"][a] = datetime.fromtimestamp(int(index))

    def delete_columns(self, data):
        del data["comments"], data["caption"], data["likes"], data["link"], data["location"], \
            data["user"], data["user_has_liked"], data["users_in_photo"], data["attribution"]


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

    def insert_new_record(self, data, index):
        query = """INSERT INTO SocMedia_Instagram_Posts (	post_id ,
										PostingDate ,
                                        PostType ,
                                        Caption ,
                                        Filter ,
                                        Hashtags ,
                                        PhotoTags ,
                                        Comments ,
                                        Likes ,
                                        Width ,
                                        Height)
                    SELECT 	'{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}""".format(data["id"][index], data["datetime"][index], data["type"][index], data["title"][index], data["filter"][index], data["tags"][index], data["phototags"][index], int(data["comments_count"][index]), int(data["likes_count"][index]), int(data["width"][index]), int(data["height"][index]))
        print(query)
        cursor.execute(query)
        conn.commit()
        print('inserted')

    def update_insta_data(self, data, index):
        query = """UPDATE 	SocMedia_Instagram_Posts s
                   SET		Likes = {},
                            Comments = {}
                   WHERE post_id = '{}';""".format(int(data["comments_count"][index]), int(data["likes_count"][index]),
                                                   data["id"][index])
        print(query)
        cursor.execute(query)
        conn.commit()
        print('updated')


def check_id(id):

    query = """SELECT post_id
               FROM SocMedia_Instagram_Posts
               WHERE post_id = {}""".format("'"+id+"'")
    cursor.execute(query)
    if not cursor.fetchall():
        return 0
    else:
        return 1


if __name__ == '__main__':
    parser = ConfigParser()
    parser.read("apikeys.ini")
    reader = codecs.getreader("utf-8")

    insta_uri = parser.get('INSTAGRAM', 'URI')
    insta_id = parser.get('INSTAGRAM', 'CLIENTID')
    insta_secret = parser.get('INSTAGRAM', 'CLIENTSECRET')
    insta_token = parser.get('INSTAGRAM', 'ACCESSTOKEN')

    reader = codecs.getreader("utf-8")

    api_endpoint = 'https://api.instagram.com/v1/users/self/media/recent'
    api = insta(client_id=insta_id, client_secret=insta_secret)

    insta_graph_url = api_endpoint+'?access_token=&access_token='+insta_token
    df = pd.DataFrame(json.loads(urlopen(insta_graph_url).read())["data"])

    i = InstagramFrame()

    i.comments_count(df)
    i.init_data(data=df)

    sql = SQL_Commands()

    for a, index in enumerate(elem for elem in df["id"]):
        if index == '':
            pass
        else:
            if check_id(index) == 0:
                pass
                sql.insert_new_record(df, a)
            elif check_id(index) == 1:
                pass
                sql.update_insta_data(df, a)
