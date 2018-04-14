import pandas as pd
from configparser import ConfigParser
import os
import pyodbc as mysql
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from time import sleep
from datetime import timedelta, datetime

class TwitterBrowser:
    def __init__(self):
        global LOGIN, PASSWORD, browser

        chrome_options = Options()
        prefs = {"download.default_directory": str(os.getcwd())}

        # chrome_options.add_argument("--headless")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--incognito")
        chrome_options.add_experimental_option("prefs", prefs)

        chrome_driver = os.getcwd() + "\\chromedriver.exe"

        browser = webdriver.Chrome(chrome_options=chrome_options, executable_path=chrome_driver)

        parser = ConfigParser()
        parser.read("apikeys.ini")
        LOGIN = parser.get('TWITTER', 'USERNAME')
        PASSWORD = parser.get('TWITTER', 'PASSWORD')

    def get_url(self, url, sec):
        load_page = browser.get(url)
        try:
            WebDriverWait(browser, timeout=sec)
        except TimeoutException:
            print('TIMED OUT!')
        return load_page

    def login(self):
        twt.get_url('https://twitter.com/login', 5)
        browser.find_element_by_xpath('//*[@id="page-container"]/div/div[1]/form/fieldset/div[1]/input').send_keys(
            LOGIN)
        browser.find_element_by_xpath('//*[@id="page-container"]/div/div[1]/form/fieldset/div[2]/input').send_keys(
            PASSWORD)
        WebDriverWait(browser, 6)
        browser.find_element_by_xpath('//*[@id="page-container"]/div/div[1]/form/div[2]/button').click()

    def tweet_analytics(self):
        twt.get_url('https://analytics.twitter.com/user/' + LOGIN + '/tweets', 5)
        sleep(5)
        browser.find_element_by_xpath('//*[@id="export"]/button/span[2]').click()
        sleep(60*5)

    def audience_analytics(self):
        twt.get_url('https://analytics.twitter.com/user/' + LOGIN + '/tweets', 5)
        browser.find_element_by_css_selector('#SharedNavBarContainer > div > div > ul:nth-child(2) > li:nth-child(3) > a').click()
        sleep(5)

        df = pd.DataFrame(data={'reportdate': datetime.today().date()},index=range(1, 2, 1))

        df['total_followers'] = str(browser.find_element_by_xpath('//*[@id="chart"]/div[1]/h3/b').text).replace(',', '')

        browser.find_element_by_xpath('//*[@id="audience-insights-panels"]/ul/li[2]/button').click()
        df['male_followers'] = float(str(browser.find_element_by_xpath('//*[@id="audience-insights-panels"]/div[2]/div/div[1]/div[1]/div[2]/div[2]/div[1]/h4').text)[:2])/100

        df['female_followers'] = float(str(browser.find_element_by_xpath('//*[@id="audience-insights-panels"]/div[2]/div/div[1]/div[1]/div[2]/div[2]/div[2]/h4').text)[:2])/100

        for i in range(2, 11, 1):
            df[str(browser.find_element_by_xpath('//*[@id="audience-insights-panels"]/div[2]/div/div[3]/div[3]/div[2]/table/tbody/tr['+str(i)+']/td[1]').text).replace(', GB', '')] = float(str(browser.find_element_by_xpath('//*[@id="audience-insights-panels"]/div[2]/div/div[3]/div[3]/div[2]/table/tbody/tr['+str(i)+']/td[2]').text).replace('%', ''))/100

        return df

class SQLDriver:
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
                             database='NovaraWH',
                             driver='MySQL ODBC 5.3 ANSI Driver')
        cursor = conn.cursor()

    def data_upload(self, tweet_data, account_data):
        for index, row in enumerate(elem for elem in tweet_data["Tweet permalink"]):
            cursor.execute("SELECT Tweetpermalink FROM SocMedia_Twitter_Tweets WHERE Tweetpermalink = '{}'".format(row))
            if cursor.fetchall() == []:
                sql.tweet_import_insert(tweet_data, index)
            else:
                sql.tweet_import_update(tweet_data, index)
        print(account_data)
        sql.account_import(account_data)

    def tweet_import_insert(self, data, ind):
        query = """INSERT INTO SocMedia_Twitter_Tweets (time	,
                                                Tweetpermalink	,
                                                Tweettext	,
                                                impressions	,
                                                engagements	,
                                                engagementrate		,
                                                retweets	,
                                                replies		,
                                                likes		,
                                                userprofileclicks,
                                                urlclicks	,
                                                hashtagclicks,
                                                detailexpands,
                                                permalinkclicks	,
                                                mediaviews		,
                                                mediaengagements)
            SELECT 	'{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}""".format(datetime.strptime(str(data["time"][ind]), '%Y-%m-%d %H:%M %z'), data["Tweet permalink"][ind], str(data["Tweet text"][ind]).replace("'", ""), int(data["impressions"][ind]), int(data["engagements"][ind]), float(data["engagement rate"][ind]), int(data["retweets"][ind]), int(data["replies"][ind]), int(data["likes"][ind]), int(data["user profile clicks"][ind]), int(data["url clicks"][ind]), int(data["hashtag clicks"][ind]), int(data["detail expands"][ind]), int(data["permalink clicks"][ind]), int(data["media views"][ind]), int(data["media engagements"][ind]))
        cursor.execute(query)
        conn.commit()

    def tweet_import_update(self, data, ind):
        query = """UPDATE 	SocMedia_Twitter_Tweets s
                   SET		impressions	= {},
                            engagements	= {},
                            engagementrate	= {},
                            retweets		= {},
                            replies		= {},
                            likes		= {},
                            userprofileclicks	= {},
                            urlclicks		= {},
                            hashtagclicks	= {},
                            detailexpands	= {},
                            permalinkclicks		= {},
                            mediaviews			= {},
                            mediaengagements	= {}
                   WHERE Tweetpermalink = '{}'""".format(int(data["impressions"][ind]), int(data["engagements"][ind]), float(data["engagement rate"][ind]), int(data["retweets"][ind]), int(data["replies"][ind]), int(data["likes"][ind]), int(data["user profile clicks"][ind]), int(data["url clicks"][ind]), int(data["hashtag clicks"][ind]), int(data["detail expands"][ind]), int(data["permalink clicks"][ind]), int(data["media views"][ind]), int(data["media engagements"][ind]), data["Tweet permalink"][ind])
        cursor.execute(query)
        conn.commit()

    def account_import(self, df):
        query = """INSERT INTO SocMedia_Twitter_Account (   ReportDate,
                                                            total_followers,
                                                            male_followers  ,
                                                            female_followers,
                                                            Greater_London  ,
                                                            North_West_England,
                                                            South_East_England,
                                                            East_England   ,
                                                            South_West_England  ,
                                                            Scotland  ,
                                                            Yorkshire_and_The_Humber,
                                                            West_Midlands ,
                                                            Wales)
          SELECT 	'{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}""".format(datetime.strptime(str(df["reportdate"][1]), '%Y-%m-%d'), df["total_followers"][1], float(df["male_followers"][1]), float(df["female_followers"][1]), float(df["Greater London"][1]), float(df["South East England"][1]), float(df["North West England"][1]), float(df["East England"][1]), float(df["South West England"][1]), float(df["Yorkshire and The Humber"][1]), float(df["Scotland"][1]), float(df["West Midlands"][1]), float(df["Wales"][1]))
        cursor.execute(query)
        conn.commit()

if __name__ == "__main__":
    twt = TwitterBrowser()
    twt.login()
    twt.tweet_analytics()

    tweet_df = pd.read_csv(os.getcwd()+'\\tweet_activity_metrics_'+LOGIN+'_'+(datetime.today()+timedelta(-27)).strftime('%Y%m%d')+'_'+(datetime.today()+timedelta(1)).strftime('%Y%m%d')+'_en.csv')
    for i, index in enumerate(elem for elem in tweet_df):
        if str(index).__contains__('promoted'):
            del tweet_df[index]
    sql = SQLDriver()
    sql.data_upload(tweet_df, twt.audience_analytics())
