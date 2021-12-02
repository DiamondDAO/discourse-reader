#!/usr/bin/env python

import requests
import json
import numpy as np
from tqdm import tqdm as tqdm
import os
import argparse
import time


class DiscourseScrapper:
    def __init__(self, base_url, outpath=".", threads=8):
        self.base_url = base_url
        if self.base_url[-1] != "/":
            self.base_url += "/"
        self.outpath = outpath
        try:
            os.mkdir(os.path.join(self.outpath))
        except:
            pass

        try:
            os.mkdir(os.path.join(self.outpath, "categories"))
        except:
            pass

    def parse(self, categories=None):
        users = self.get_users()

        with open(os.path.join(self.outpath, "users.json"), "w") as f:
            f.write(json.dumps(users))

        users = self.sort_users(users)

        if not categories:
            categories = self.get_categories()

            with open(os.path.join(self.outpath, "categories.json"), "w") as f:
                f.write(json.dumps(categories))

        for category in tqdm(categories, position=0):
            self.parse_category(category)

    def parse_category(self, category):
        topics = self.get_topics(category, data=[])
        category["topics"] = topics
        posts = {}
        for topic in tqdm(topics, position=1):
            posts[topic["id"]] = self.get_posts(topic["id"])
            likes = {}
            for post in posts[topic["id"]]["post_stream"]["posts"]:
                likes[post["id"]] = self.get_likes(post["id"])
            posts[topic["id"]]["likes"] = likes
        category["posts"] = posts
        with open(os.path.join(self.outpath, "categories", "{}.json".format(category["slug"])), "w") as f:
            f.write(json.dumps(category))
        return category

    def sort_users(self, users):
        for idx, user in enumerate(users):
            if ".eth" in str(user["user"]["name"]) or ".eth" in str(user["user"]["username"]):
                users[idx]["ethUser"] = True
            elif "0x" in str(user["user"]["name"]) or "0x" in str(user["user"]["username"]):
                users[idx]["ethUser"] = True
            else:
                users[idx]["ethUser"] = False

        return users

    def get_users(self, data=[], page=0, retry=0):
        if retry > 10:
            return data
        try:
            r = requests.get(self.base_url + "directory_items.json?period=all&page={}".format(page))
            content = json.loads(r.content.decode())
            data += content["directory_items"]
            if len(data) < content["meta"]["total_rows_directory_items"]:
                return self.get_users(data=data, page=page + 1)
            return data
        except:
            time.sleep(retry)
            return self.get_users(data=data, page=page, retry=retry + 1)

    def get_categories(self, data=[], retry=0):
        if retry > 10:
            return data
        try:
            r = requests.get(self.base_url + "categories.json")
            content = json.loads(r.content.decode())
            data += content["category_list"]["categories"]
            return data
        except:
            time.sleep(retry)
            return self.get_categories(retry=retry + 1)

    def get_topics(self, category, data=[], page=0, retry=0):
        if retry > 10:
            return data
        try:
            r = requests.get(self.base_url + "c/{}/{}.json?page={}".format(category["slug"], category["id"], page))
            content = json.loads(r.content.decode())
            data += content["topic_list"]["topics"]
            if len(content["topic_list"]["topics"]) > 0:
                return self.get_topics(category, data=data, page=page + 1)
            return data
        except:
            time.sleep(retry)
            return self.get_topics(category, data=data, page=page, retry=retry + 1)

    def get_posts(self, topic_id, retry=0):
        if retry > 10:
            return {"post_stream": {"posts": []}}
        try:
            r = requests.get(self.base_url + "t/{}.json".format(topic_id))
            content = json.loads(r.content.decode())
            return content
        except:
            time.sleep(retry)
            return self.get_posts(topic_id, retry=retry + 1)

    def get_likes(self, post_id, retry=0):
        if retry > 10:
            return []
        try:
            r = requests.get(self.base_url + "post_action_users.json?id={}&post_action_type_id=2".format(post_id))
            content = json.loads(r.content.decode())
            return content
        except:
            time.sleep(retry)
            return self.get_likes(post_id, retry=retry + 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discourse Forum Parser.")

    parser.add_argument(
        "-b",
        "--url",
        metavar="base_url",
        type=str,
        required=True,
        help="The base url for the Discourse forum to scrape data from, starting with http:// or https://",
    )
    parser.add_argument(
        "-o",
        "--out",
        metavar="out_path",
        type=str,
        required=True,
        help="The location for the folder containing the result of the scrapping.",
    )
    parser.add_argument(
        "-s",
        "--slug",
        metavar="category_slug",
        type=str,
        help="IF restricting to one category only, the slug of that category. (requries -i to be set as well)",
    )
    parser.add_argument(
        "-i",
        "--id",
        metavar="category_id",
        type=str,
        help="IF restricting to one category only, the id of that category. (requries -s to be set as well)",
    )

    args = parser.parse_args()

    scraper = DiscourseScrapper(args.url, args.out)
    if args.slug and args.id:
        category = {"slug": args.slug, "id": args.id}
        scraper.parse(categories=[category])
    else:
        scraper.parse()
