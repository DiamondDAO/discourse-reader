import boto3
import json
from datetime import datetime
from tqdm import tqdm as tqdm
import sys

sys.path.append(__file__)
from helpers.cleaning import clean_category_file, clean_users

if __name__ == "__main__":

    now = datetime.now()
    s3 = boto3.resource("s3")

    user_file_name = f"discourse/daohaus/users/{now.strftime('%m-%d-%Y')}/users.json"
    user_content_object = s3.Object("chainverse", user_file_name)
    user_file_content = user_content_object.get()["Body"].read().decode("utf-8")
    all_users = json.loads(user_file_content)

    all_cleaned_users = clean_users(all_users)
    all_cleaned_categories = []
    all_cleaned_topics = []
    all_cleaned_posts = []

    chainverse_bucket = s3.Bucket("chainverse")
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

    with open("../../results/discourse/results/ingest/test_categories.json", "w") as file:
        json.dump(all_cleaned_categories, file)

    with open("../../results/discourse/results/ingest/test_users.json", "w") as file:
        json.dump(all_cleaned_users, file)

    with open("../../results/discourse/results/ingest/test_topics.json", "w") as file:
        json.dump(all_cleaned_topics, file)

    with open("../../results/discourse/results/ingest/test_posts.json", "w") as file:
        json.dump(all_cleaned_posts, file)
