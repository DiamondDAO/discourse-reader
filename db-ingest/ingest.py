import boto3
import json
from datetime import datetime
from tqdm import tqdm as tqdm
import sys

sys.path.append(__file__)


def clean_users(raw_users):
    user_entry_keys = [
        "user_id",
        "username",
        "name",
        "time_read",
        "likes_received",
        "likes_given",
        "topics_entered",
        "topic_count",
        "post_count",
        "posts_read",
        "days_visited",
        "avatar_template",
        "title",
        "ethUser",
    ]

    final_users = []
    for raw_user in tqdm(raw_users, position=0):
        clean_user = {}
        final_user = {}
        user_subdict = raw_user["user"]
        raw_user.pop("user")
        user_subdict.pop("id")
        for key, value in user_subdict.items():
            clean_user[key] = value
        for key, value in raw_user.items():
            clean_user[key] = value
        clean_user["user_id"] = clean_user["id"]
        for key in user_entry_keys:
            final_user[key] = clean_user[key]
        final_users.append(final_user)

    return final_users


def clean_topics(raw_topics):
    topic_entry_keys = [
        "topic_id",
        "category_id",
        "title",
        "fancy_title",
        "slug",
        "posts_count",
        "reply_count",
        "image_url",
        "created_at",
        "bumped",
        "bumped_at",
        "archetype",
        "unseen",
        "pinned",
        "excerpt",
        "visible",
        "closed",
        "archived",
        "bookmarked",
        "liked",
        "tags",
        "views",
        "like_count",
        "has_summary",
        "pinned_globally",
        "featured_link",
    ]

    final_topics = []
    for topic in tqdm(raw_topics, position=1):
        clean_topic = {}
        final_topic = {}
        for key, value in topic.items():
            clean_topic[key] = value
        clean_topic["topic_id"] = clean_topic["id"]
        for key in topic_entry_keys:
            try:
                final_topic[key] = clean_topic[key]
            except:
                final_topic[key] = None
        final_topics.append(final_topic)

    return final_topics


def clean_category(raw_category):
    category_entry_keys = [
        "category_id",
        "name",
        "slug",
        "topic_count",
        "post_count",
        "position",
        "description",
        "description_text",
        "description_excerpt",
        "topic_url",
        "read_restricted",
        "notification_level",
        "has_children",
        "num_featured_topics",
        "minimum_required_tags",
        "topics_day",
        "topics_week",
        "topics_month",
        "topics_year",
        "topics_all_time",
        "uploaded_logo",
        "uploaded_background",
    ]
    clean_category = {}
    final_category = {}
    for key, value in raw_category.items():
        clean_category[key] = value
    clean_category["category_id"] = clean_category["id"]
    for key in category_entry_keys:
        try:
            final_category[key] = clean_category[key]
        except:
            final_category[key] = None

    return final_category


def clean_posts(raw_posts):
    post_entry_keys = [
        "post_id",
        "topic_id",
        "user_id",
        "name",
        "username",
        "created_at",
        "cooked",
        "post_number",
        "post_type",
        "updated_at",
        "reply_count",
        "reply_to_post_number",
        "quote_count",
        "incoming_link_count",
        "reads",
        "readers_count",
        "score",
        "read",
        "bookmarked",
        "admin",
        "staff",
        "hidden",
        "deleted_at",
        "user_deleted",
        "edit_reason",
        "like_count",
    ]
    final_posts = []
    for topic_id in raw_posts.keys():
        current_post_stream = raw_posts[topic_id]["post_stream"]
        likes = raw_posts[topic_id]["likes"]
        for post in current_post_stream["posts"]:
            cleaned_post = {}
            final_post = {}
            for key, value in post.items():
                cleaned_post[key] = value
            cleaned_post["post_id"] = cleaned_post["id"]
            try:
                likes_list = likes[str(topic_id)]["post_action_users"]
            except:
                likes_list = []
            cleaned_post["like_count"] = len(likes_list)
            for key in post_entry_keys:
                try:
                    final_post[key] = cleaned_post[key]
                except:
                    final_post[key] = None
            final_posts.append(final_post)

    return final_posts


def clean_category_file(raw_category):
    posts = raw_category["posts"]
    topics = raw_category["topics"]
    raw_category.pop("posts")
    raw_category.pop("topics")
    cleaned_topics = clean_topics(topics)
    cleaned_category = clean_category(raw_category)
    cleaned_posts = clean_posts(posts)

    return cleaned_category, cleaned_topics, cleaned_posts


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
