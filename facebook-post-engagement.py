#!/usr/bin/python
# coding: utf-8

from    urllib.request import Request, urlopen
from    datetime import date, timedelta
import json
import csv
import codecs
import tinys3

# Facebook API Connection
page_id = "404716342902872"
access_token = 'EAAGfycPwZBg8BAIDd74LXXTytZAKCwzolYZCHxcRRnjeoFcnZCZCKJOKPoEgoN9WR36viFENPlStQyWclwNa2oUrcfowmWVAPhfizcAaKKDEnGGftFZCEGYmSl2BFVp5OkGL6VmSQpOiDk3ipzeBQNgdWRPDNdNWHw29N8fjNn5AZDZD'

api_endpoint = "https://graph.facebook.com/"
fb_graph_url = api_endpoint + page_id + "?fields=posts%7Bid%2Ccreated_time%2Cname%2Cshares%2Clikes%2Ccomments%7D&access_token=" + access_token
reader = codecs.getreader("utf-8")

#   Get JSON data

page_data = None

try:
    api_request = Request(fb_graph_url)
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
                        "likes",
                        "comments",
                        "shares"])

# Create connection to S3 bucket
conn = tinys3.Connection('AKIAJGISY5PP7ULQGLMQ', 'lDU00TlMCN7QVQ89BtVHsY09JhN6rYNsy+f5m5+4', tls=True)

# reset post count
i = 0

for post in page_data["posts"]["data"]:

    # reset variables
    post_id = ""
    title = ""
    post_type = ""
    created_time = ""
    likes = 0
    comments = 0
    shares = 0

    # post_id
    post_id = page_data["posts"]["data"][i]["id"]

    # created_time
    created_time = page_data["posts"]["data"][i]["created_time"]

    # strip suffix & generate post_type
    title = page_data["posts"]["data"][i]["name"]
    if title == "Novara Media":
        post_type = "Video"
    if title.__contains__("Timeline Photos"):
        post_type = "Image"
    else:
        title = title.replace(" | Novara Media", "")
        post_type = "Webpage"

    # post likes
    try:
        likes_staging = page_data["posts"]["data"][i]["likes"]["data"]
        for l in likes_staging:
            likes += 1
    except (TypeError, KeyError):
        likes = 0

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

    write_outfile.writerow([post_id, post_type, created_time, title, likes, comments, shares])

    i += 1
