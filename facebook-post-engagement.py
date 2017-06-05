#!/usr/bin/python
# coding: utf-8

import codecs
import csv
import json
import os
import dateutil.parser as dt
from   configparser import ConfigParser
from   datetime import date, timedelta
from   urllib.request import Request, urlopen
from   boto import boto
from   boto.s3.key import Key


def post_reactions(data, reaction):
    global like, angry, love, sad, haha, wow

    like = 0
    love = 0
    sad = 0
    angry = 0
    haha = 0
    wow = 0

    try:
        reaction_staging = data["data"]
    except KeyError:
        reaction_staging = data

    for l in reaction_staging:
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
    if reaction == like:
        return like
    elif reaction == angry:
        return angry
    elif reaction == love:
        return love
    elif reaction == sad:
        return sad
    elif reaction == haha:
        return haha
    elif reaction == wow:
        return wow


def populate_csv(url):
    global page_data, like, love, haha, wow, angry, sad

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
            post_type = "Video"
            try:
                title = page_data["videos"]["data"][i]["title"]
            except KeyError:
                title = "Untitled Video"
        elif title.__contains__("Timeline Photos"):
            post_type = "Image"
        else:
            title = title.replace(" | Novara Media", "")
            post_type = "Webpage"

            # post likes
        try:
            likes_staging = page_data["posts"]["data"][i]["reactions"]

            like = post_reactions(likes_staging, like)
            love = post_reactions(likes_staging, love)
            sad = post_reactions(likes_staging, sad)
            angry = post_reactions(likes_staging, angry)
            haha = post_reactions(likes_staging, haha)
            wow = post_reactions(likes_staging, wow)
        # print(page_data["posts"]["data"][i]["reactions"]["paging"]["next"])
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


def upload_to_s3(aws_access_key_id, aws_secret_access_key, file, bucket, key, content_type=None):
    try:
        size = os.fstat(file.fileno()).st_size
    except:
        # Not all file objects implement fileno(),
        # so we fall back on this
        file.seek(0, os.SEEK_END)
        size = file.tell()
    region_host = 's3.eu-west-2.amazonaws.com'
    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    bucket_name = conn.get_bucket(bucket, validate=True)
    k = Key(bucket_name)
    k.key = key
    if content_type:
        k.set_metadata('Content-Type', content_type)
    sent = k.set_contents_from_file(file)

    # Rewind for later use
    file.seek(0)

    if sent == size:
        return True
    return False


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
filepath = 'C:\\Users\\Fraser\\Desktop\\'
filename = yesterday.strftime('%Y%m%d') + '_facebook_postengagement.csv'
outfile = open(filename, 'w', newline="")
write_outfile = csv.writer(outfile)

like = 0
angry = 0
love = 0
sad = 0
haha = 0
wow = 0

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

# AWS S3 Connection
parser.read("awskeys.ini")
AWS_ACCESS_KEY = parser.get('ACCESSKEY', 'novara-storage')
AWS_ACCESS_SECRET_KEY = parser.get('SECRETKEY', 'novara-storage')
file = open(filename, 'r+')
key = file.name
bucket = 'novara-cloud-storage'

populate_csv(fb_graph_url)
#upload_to_s3(AWS_ACCESS_KEY, AWS_ACCESS_SECRET_KEY, file, bucket, key)
