#!/usr/bin/python
# coding: utf-8

import codecs
import json
from configparser import ConfigParser
from datetime import date, timedelta, datetime
from urllib.request import urlopen
import dateutil.parser as dt
import mysql.connector as mysql
import pandas as pd

class Page_Data:
    def __init__(self):
        global parser
        parser = ConfigParser()
        parser.read("apikeys.ini")
        pd.options.mode.chained_assignment = None

    def page_data_processing(self, token, api, from_date):
        data_staging = pd.DataFrame(columns=["ReportDate"], data=pd.date_range(from_date, date.today()-timedelta(1)))

        metric = ['page_fans', 'page_fan_adds_unique',
                  'page_fan_removes_unique', 'page_posts_impressions', 'page_posts_impressions_unique',
                  'page_consumptions', 'page_consumptions_unique',
                  'page_consumptions_by_consumption_type', 'page_consumptions_by_consumption_type_unique']

        for m in metric:
            fb_graph_url = api+'v2.12/novaramedia/insights/'+m+'?since='+str(from_date)[:10]+'&access_token='+token
            json_output = json.loads(urlopen(fb_graph_url).read())["data"][0]["values"]
            json_data_frame = pd.DataFrame(json_output)
            if type(json_data_frame["value"][0]).__name__ in ('int64', 'float64'):
                data_staging[m] = 0
            elif type(json_data_frame["value"][0]).__name__ == 'dict':
                for index, title in enumerate(elem for elem in json_data_frame["value"][0]):
                    if title not in data_staging:
                        data_staging[title] = 0
                    else:
                        data_staging[str(title)+'_unique'] = 0
            else:
                pass

            for index, row in enumerate(json_data_frame["end_time"]):
                json_data_frame["end_time"][index] = dt.parse(json_data_frame["end_time"][index])

                for i, r in enumerate(elem for elem in data_staging["ReportDate"]):
                    if data_staging["ReportDate"][i].date() == json_data_frame["end_time"][index].date():
                        # SPLIT BY DATA TYPE AGAIN
                        if type(json_data_frame["value"][0]).__name__ == 'int64':
                            data_staging[m][i] = json_data_frame["value"][index] # NORMAL INT COLUMNS
                        elif type(json_data_frame["value"][0]).__name__ == 'dict':
                            for title in enumerate(elem for elem in json_data_frame["value"][index]):
                                if m == 'page_consumptions_by_consumption_type_unique':
                                    data_staging[str(title[1])+'_unique'][i] = json_data_frame["value"][index][title[1]]
                                else:
                                    data_staging[title[1]][i] = json_data_frame["value"][index][title[1]]
            del json_data_frame
        return data_staging

    def demographics_processing(self, token, api, from_date):
        data_staging = pd.DataFrame(columns=["ReportDate"], data=pd.date_range(from_date, date.today()-timedelta(1)))

        metric = ['page_fans_city', 'page_fans_country', 'page_fans_gender_age']

        for m in metric:
            fb_graph_url = api+'v2.12/novaramedia/insights/'+m+'?since='+str(from_date)[:10]+'&access_token='+token
            json_output = json.loads(urlopen(fb_graph_url).read())
            json_data_frame = pd.DataFrame(json_output["data"][0]["values"])
            country_codes = pd.DataFrame(json.loads(open('fb_country.json').read())["data"])
            for i, index in enumerate(dict(elem) for elem in json_data_frame["value"]):
                if m == 'page_fans_country':
                    for key in index:
                        cursor.execute("""INSERT INTO SocMedia_Facebook_Countries (ReportDate, Country, Likes)
                                          SELECT '{}', '{}', {}""".format(datetime.strptime(json_data_frame["end_time"][i], '%Y-%m-%dT%H:%M:%S%z').date(), country_codes.loc[country_codes['country_code'].values == key]["name"].values[0], index[key]))
                        conn.commit()

                if m == 'page_fans_city':
                    for key in index:
                        Page_Data().city_import(df=json_data_frame, index=index, key=key, i=i)

                if m == 'page_fans_gender_age':
                    for key in index:
                        report_date = datetime.strptime(json_data_frame["end_time"][i], '%Y-%m-%dT%H:%M:%S%z').date()
                        if key[0] == 'F':
                            gender = 'Female'
                        elif key[0] == 'M':
                            gender = 'Male'
                        elif key[0] == 'U':
                            gender = 'Unknown'
                        else:
                            pass
                        query = """INSERT INTO SocMedia_Facebook_Demogrpahics_Age (ReportDate, Gender, AgeRange, Likes)
                                           SELECT '{}', '{}', '{}', {}""".format(report_date, gender, str(key[2:]), index[key])
                        cursor.execute(query)
                        conn.commit()

                        cursor.execute("""  UPDATE SocMedia_Facebook_Demogrpahics_Age dest,
                                          (   SELECT today.*, today.Likes-yesterday.Likes AS GenNewLikes
                                              FROM SocMedia_Facebook_Demogrpahics_Age today
                                              LEFT JOIN SocMedia_Facebook_Demogrpahics_Age yesterday
                                                ON date_add(today.ReportDate, INTERVAL -1 DAY) = yesterday.ReportDate
                                                AND today.Gender = yesterday.Gender
                                                AND today.AgeRange = yesterday.AgeRange) src
                                            SET dest.NewLikes = src.GenNewLikes WHERE dest.ReportDate = src.ReportDate
                                                                                AND   dest.Gender = src.Gender
                                                                                AND   dest.AgeRange = src.AgeRange""")
                        conn.commit()
    def city_import(self, df, index, key, i):
        if str(key).count(',') == 2:
            query = """INSERT INTO SocMedia_Facebook_Cities (ReportDate, Country, City, Likes)
                       SELECT '{}', '{}', '{}', {}""".format(datetime.strptime(df["end_time"][i], '%Y-%m-%dT%H:%M:%S%z').date(), str(key).replace(key[str(key).find(','):], ''), str(key)[str(key).find(',', str(key).find(',')+1)+2:], index[key])
        elif len(str(key)[str(key).find(', ')+2:]) == 2:
            query = """INSERT INTO SocMedia_Facebook_Cities (ReportDate, Country, City, Likes)
                       SELECT '{}', '{}', '{}', {}""".format(datetime.strptime(df["end_time"][i], '%Y-%m-%dT%H:%M:%S%z').date(), str(key)[:str(key).find(',')], 'United States of America', index[key])
        else:
            query = """INSERT INTO SocMedia_Facebook_Cities (ReportDate, Country, City, Likes)
                       SELECT '{}', '{}', '{}', {}""".format(datetime.strptime(df["end_time"][i], '%Y-%m-%dT%H:%M:%S%z').date(), str(key)[:str(key).find(',')], str(key)[str(key).find(', ')+2:], index[key])
        cursor.execute(query)
        conn.commit()

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

    def last_import(self):
        global max_date
        cursor.execute("""SELECT IFNULL(MAX(reportdate),'2016-01-01') FROM SocMedia_Facebook_Page;""")
        max_date = cursor.fetchall()[0][0]
        return max_date

    def upload_data(self, data):
        for index, date in enumerate(data["ReportDate"]):
            if data["page_fans"][index] == 0:
                pass
            elif data["ReportDate"][index].date() <= pd.to_datetime(max_date).date():
                pass
            else:
                cursor.execute('''INSERT INTO SocMedia_Facebook_Page (	reportdate,
                                                                        page_fans,
                                                                        page_fan_adds_unique,
                                                                        page_fan_removes_unique,
                                                                        page_posts_impressions ,
                                                                        page_posts_impressions_unique ,
                                                                        page_consumptions ,
                                                                        page_consumptions_unique ,
                                                                        video_play ,
                                                                        other_clicks,
                                                                        photo_views ,
                                                                        link_clicks ,
                                                                        button_clicks  ,
                                                                        video_play_unique ,
                                                                        other_clicks_unique,
                                                                        photo_view_unique ,
                                                                        link_clicks_unique ,
                                                                        button_clicks_unique )
                                  SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s''',
                               [pd.to_datetime(data['ReportDate'][index]).to_pydatetime(),
                                int(data['page_fans'][index]),
                                int(data["page_fan_adds_unique"][index]),
                                int(data["page_fan_removes_unique"][index]),
                                int(data["page_posts_impressions"][index]),
                                int(data["page_posts_impressions_unique"][index]),
                                int(data["page_consumptions"][index]),
                                int(data["page_consumptions_unique"][index]),
                                int(data["video play"][index]),
                                int(data["other clicks"][index]),
                                int(data["photo view"][index]),
                                int(data["link clicks"][index]),
                                int(data["button clicks"][index]),
                                int(data["video play_unique"][index]),
                                int(data["other clicks_unique"][index]),
                                int(data["photo view_unique"][index]),
                                int(data["link clicks_unique"][index]),
                                int(data["button clicks_unique"][index])])
                conn.commit()

if __name__ == '__main__':
    pg_d = Page_Data()
    sql = SQL_Commands()

    access_token = parser.get('FACEBOOK', 'ACCESS_TOKEN')
    api_endpoint = "https://graph.facebook.com/"
    reader = codecs.getreader("utf-8")

    from_date = pd.to_datetime(sql.last_import(), yearfirst=True)
    pg_d.demographics_processing(token=access_token, api=api_endpoint, from_date=from_date)
    sql.upload_data(pg_d.page_data_processing(token=access_token, api=api_endpoint, from_date=from_date))
