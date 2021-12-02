#!/usr/bin/env python

import requests
import json
from tqdm import tqdm as tqdm
import argparse
import time
import boto3
from datetime import datetime


class DiscourseScrapper:
    def __init__(self, base_url, s3, now, threads=8):
        self.base_url = base_url
        if self.base_url[-1] != "/":
            self.base_url += "/"
        self.s3 = s3
        self.date = now

    def parse(self, categories=None):
        users = self.get_users()
        users = self.sort_users(users)

        # save all users information as a json in s3
        user_file_name = f"discourse/daohaus/users/{self.date.strftime('%m-%d-%Y')}/users.json"
        s3object = self.s3.Object("chainverse", user_file_name)
        s3object.put(Body=(bytes(json.dumps(users).encode("UTF-8"))))
        print("users json saved to s3")

        if not categories:
            categories = self.get_categories()

            # save all category broad information in s3
            all_category_file_name = (
                f"discourse/daohaus/categories/{self.date.strftime('%m-%d-%Y')}/all_categories.json"
            )
            s3object = self.s3.Object("chainverse", all_category_file_name)
            s3object.put(Body=(bytes(json.dumps(categories).encode("UTF-8"))))
            print("all categories json saved to s3")

        for category in tqdm(categories, position=0):
            self.parse_category(category)
        print("all category files saved to s3")

    # parse a category by returning all topics associated with the category, then iteratring through these topics to recover all posts and likes
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

        # save individiual category information (topics, posts, metadata) in an individual file in s3
        category_file_name = f"discourse/daohaus/categories/{self.date.strftime('%m-%d-%Y')}/category_files/{category['id']}-{category['slug']}.json"
        s3object = self.s3.Object("chainverse", category_file_name)
        s3object.put(Body=(bytes(json.dumps(category).encode("UTF-8"))))

        return category

    # iterate through users and mark them as an ethUser (depending on whether their name or username has an ENS or ethereum address)
    def sort_users(self, users):
        for idx, user in enumerate(users):
            if ".eth" in str(user["user"]["name"]) or ".eth" in str(user["user"]["username"]):
                users[idx]["ethUser"] = True
            elif "0x" in str(user["user"]["name"]) or "0x" in str(user["user"]["username"]):
                users[idx]["ethUser"] = True
            else:
                users[idx]["ethUser"] = False

        return users

    # get all users for the forum via API call
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

    # get all categories for the forum via API call
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

    # get all topics for a single category via API call
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

    # get all posts for a topic by topic-id via API call
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

    # get likes for a post by post-id via API call
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

    # set up AWS storage (s3)
    s3 = boto3.resource("s3")
    now = datetime.now()
    scraper = DiscourseScrapper(args.url, s3, now)
    if args.slug and args.id:
        category = {"slug": args.slug, "id": args.id}
        scraper.parse(categories=[category])
    else:
        scraper.parse()
