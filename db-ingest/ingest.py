import boto3
import json
from datetime import datetime, timedelta
from tqdm import tqdm as tqdm
import sys
import io
import psycopg2
import os
import pandas as pd
from sqlalchemy import create_engine

sys.path.append(__file__)
from helpers.cleaning import clean_category_file, clean_users


def truncate_and_ingest(cleaned_json, cur, engine, table_name):
    df = pd.DataFrame(cleaned_json)
    cur.execute(f"Truncate {table_name}")
    output = io.StringIO()
    df.to_csv(output, sep="\t", header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, table_name, null="")
    engine.commit()
    print(f"{table_name} table saved")


if __name__ == "__main__":

    s3 = boto3.resource("s3")
    client = boto3.client("s3")
    chainverse_bucket = s3.Bucket("chainverse")

    # get latest scrape
    all_dates = []
    paginator = client.get_paginator("list_objects")
    for result in paginator.paginate(Bucket="chainverse", Prefix="discourse/daohaus/categories/", Delimiter="/"):
        for prefix in result.get("CommonPrefixes"):
            string_date = prefix.get("Prefix").split("/")[-2]
            numeric_date = datetime.strptime(string_date, "%m-%d-%Y")
            all_dates.append(numeric_date)

    now = max(all_dates)

    user_file_name = f"discourse/daohaus/users/{now.strftime('%m-%d-%Y')}/users.json"
    user_content_object = s3.Object("chainverse", user_file_name)
    user_file_content = user_content_object.get()["Body"].read().decode("utf-8")
    all_users = json.loads(user_file_content)

    all_cleaned_users = clean_users(all_users)
    all_cleaned_categories = []
    all_cleaned_topics = []
    all_cleaned_posts = []

    # iterate over individual category files, clean them, and append them to the final lists
    prefix_location = f"discourse/daohaus/categories/{now.strftime('%m-%d-%Y')}/category_files/"
    all_category_files = [
        object_summary.key for object_summary in chainverse_bucket.objects.filter(Prefix=prefix_location)
    ]
    for category_file in all_category_files:
        category_content_object = s3.Object("chainverse", category_file)
        category_file_content = category_content_object.get()["Body"].read().decode("utf-8")
        raw_category = json.loads(category_file_content)

        cleaned_category, cleaned_topics, cleaned_posts = clean_category_file(raw_category)
        all_cleaned_categories.append(cleaned_category)
        all_cleaned_topics.extend(cleaned_topics)
        all_cleaned_posts.extend(cleaned_posts)

    print("All data cleaned")

    # save cleaned data in s3
    cleaned_data_prefix = f"discourse/daohaus/cleaned_data/"

    file_name = cleaned_data_prefix + "users.json"
    s3object = s3.Object("chainverse", file_name)
    s3object.put(Body=(bytes(json.dumps(all_cleaned_users).encode("UTF-8"))))

    file_name = cleaned_data_prefix + "categories.json"
    s3object = s3.Object("chainverse", file_name)
    s3object.put(Body=(bytes(json.dumps(all_cleaned_categories).encode("UTF-8"))))

    file_name = cleaned_data_prefix + "topics.json"
    s3object = s3.Object("chainverse", file_name)
    s3object.put(Body=(bytes(json.dumps(all_cleaned_topics).encode("UTF-8"))))

    file_name = cleaned_data_prefix + "posts.json"
    s3object = s3.Object("chainverse", file_name)
    s3object.put(Body=(bytes(json.dumps(all_cleaned_posts).encode("UTF-8"))))

    # ingest cleaned data into the database
    database_dict = {
        "database": os.environ.get("POSTGRES_DB"),
        "user": os.environ.get("POSTGRES_USERNAME"),
        "password": os.environ.get("POSTGRES_PASSWORD"),
        "host": os.environ.get("POSTGRES_WRITER"),
        "port": os.environ.get("POSTGRES_PORT"),
    }
    engine = psycopg2.connect(**database_dict)
    cur = engine.cursor()

    truncate_and_ingest(all_cleaned_categories, cur, engine, "discourse.categories")
    truncate_and_ingest(all_cleaned_topics, cur, engine, "discourse.topics")
    truncate_and_ingest(all_cleaned_posts, cur, engine, "discourse.posts")
    truncate_and_ingest(all_cleaned_users, cur, engine, "discourse.users")

    # close cursor and exit
    cur.close()
    print("done")
