#!/usr/bin/python
# coding: utf-8

import codecs
import csv
import json
from   configparser import ConfigParser
from   datetime import date, timedelta
from   urllib.request import Request, urlopen
import dateutil.parser as dt


def populatecsv(url):
    #   Get JSON data

    global page_data

    page_data = None

    try:
        api_request = Request(url)
        api_response = urlopen(api_request)

        try:
            page_data = json.load(reader(api_response))
        except (ValueError, KeyError, TypeError):
            page_data = "JSON error"

    except IOError as e:
        if hasattr(e, 'code'):
            page_data = e.code
        elif hasattr(e, 'reason'):
            page_data = e.reason

    # reset post count
    i = 0
    for post in page_data["posts"]["data"]:
        # reset variables
        post_id = ""
        title = ""
        post_type = ""
        created_time = ""
        like = 0
        love = 0
        sad = 0
        angry = 0
        haha = 0
        wow = 0
        comments = 0
        shares = 0

        # post_id
        post_id = page_data["posts"]["data"][i]["id"]

        # created_time
        created_time = page_data["posts"]["data"][i]["created_time"]
        created_time = dt.parse(created_time)

        # strip suffix & generate post_type
        title = page_data["posts"]["data"][i]["name"]
        if title == "Novara Media":
            try:
                title = page_data["videos"]["data"][i]["title"]
            except KeyError:
                title = "Untitled Video"
            post_type = "Video"
        if title.__contains__("Timeline Photos"):
            post_type = "Image"
        else:
            title = title.replace(" | Novara Media", "")
            post_type = "Webpage"

            # post likes
        try:
            likes_staging = page_data["posts"]["data"][i]["reactions"]["data"]
            for l in likes_staging:
                if l["type"] == "LIKE":
                    like += 1
                elif l["type"] == "SAD":
                    sad += 1
                elif l["type"] == "ANGRY":
                    angry += 1
                elif l["type"] == "LOVE":
                    love += 1
                elif l["type"] == "HAHA":
                    haha += 1
                elif l["type"] == "WOW":
                    wow += 1
        except (TypeError, KeyError):
            like = 0
        # comments
        try:
            comments_staging = page_data["posts"]["data"][i]["comments"]["data"]
            for c in comments_staging:
                comments += 1
        except (TypeError, KeyError):
            comments = 0

        # post shares
        try:
            shares = page_data["posts"]["data"][i]["shares"]["count"]
        except KeyError:
            comments = 0

        write_outfile.writerow([post_id,
                                post_type,
                                created_time,
                                title,
                                like,
                                love,
                                sad,
                                angry,
                                haha,
                                wow,
                                comments,
                                shares])
        i += 1
    return page_data

# Facebook API Connection
parser = ConfigParser()
parser.read("apikeys.ini")
page_id = parser.get('PAGE ID', 'Facebook')
access_token = parser.get('API', 'Facebook')

api_endpoint = "https://graph.facebook.com/"
fb_graph_url = api_endpoint + page_id + "?fields=posts%7Bid%2Ccreated_time%2Cname%2Cshares%2Creactions%2Ccomments%7D%2Cvideos%7Btitle%2Cdescription%7D&access_token=" + access_token

reader = codecs.getreader("utf-8")

# Create csv file
yesterday = date.today() - timedelta(1)
filename = yesterday.strftime('%Y%m%d') + '_facebook_postengagement.csv'
outfile = open(filename, 'w', newline="")
write_outfile = csv.writer(outfile)

# Column Headers
write_outfile.writerow(["postID",
                        "post_type",
                        "created_time",
                        "title",
                        "like",
                        "love",
                        "sad",
                        "angry",
                        "haha",
                        "comments",
                        "shares"])


populatecsv(fb_graph_url)
