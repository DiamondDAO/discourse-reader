import boto3
import json
from datetime import datetime, timedelta
import sys
import io
import psycopg2
import os
import pandas as pd
from dotenv import load_dotenv
sys.path.append(__file__)
from helpers.s3 import get_matching_s3_keys, get_matching_s3_objects
from helpers.cleaning import *

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

    load_dotenv()

    s3_bucket = os.getenv('S3_BUCKET')
    s3 = boto3.resource("s3")

    database_dict = {
        "database": os.environ.get("POSTGRES_DB"),
        "user": os.environ.get("POSTGRES_USERNAME"),
        "password": os.environ.get("POSTGRES_PASSWORD"),
        "host": os.environ.get("POSTGRES_WRITER"),
        "port": os.environ.get("POSTGRES_PORT"),
    }

    pysycopg2Connection = psycopg2.connect(**database_dict)
    cur = pysycopg2Connection.cursor()

    def getRawContent(path, bucket = s3_bucket, fileType = '.json'):
        all_content = []
        for key in get_matching_s3_keys(s3_bucket, path, fileType):
            content_object = s3.Object(s3_bucket, key)
            file_content = content_object.get()["Body"].read().decode("utf-8")
            raw_content = json.loads(file_content)
            all_content.append(raw_content)
        return all_content

    def processData(tableName, path, cleaner, altData = None, bucket = s3_bucket, engine = pysycopg2Connection, cursor = cur):
        data = getRawContent(path)
        cleaned_data = cleaner(data)
        if altData:
            cleaned_data = altData
        # Write to s3
        file_prefix = f"discord/cleaned_data/"
        file_name = file_prefix + f"{tableName}.json"
        s3object = s3.Object(bucket, file_name)
        s3object.put(Body=(bytes(json.dumps(cleaned_data).encode("UTF-8"))))
        print(f"{tableName}.json loaded to s3")
        return cleaned_data
        # Write to DB
        truncate_and_ingest(cleaned_data, cur, engine, f"discord.{tableName}")

    # process users
    processData('users', 'discord/users/_', clean_users)
    processData('user_histories', 'discord/users/', clean_user_histories)
    # process guilds
    guilds = processData('guilds', 'discord/guilds/guildEntities', clean_guilds)
    processData('guild_histories', 'discord/guilds/guildEvents', clean_guild_histories)
    # process guild subdata
    for guild in guilds:
        guild_id = guild["id"]
        path = f'discord/guilds/{guild_id}/'
        # process messages
        processData('guild_messages', path + 'messages/_', clean_guild_messages)
        processData('guild_message_histories', path + 'messages/', clean_guild_message_histories)
        # process members and roles
        processData('guild_members', path + 'members/_', clean_guild_members)
        guild_member_histories_data = getRawContent(path + 'members')
        cleaned_mem_histories, cleaned_mem_roles = clean_guild_member_histories(guild_member_histories_data)
        processData('guild_member_histories', path + 'members/', clean_guild_member_histories, cleaned_mem_histories)
        processData('guild_member_roles', path + 'members/', clean_guild_member_histories, cleaned_mem_roles)
        processData('guild_roles', path + 'roles/_', clean_roles)
        processData('guild_role_histories', path + 'roles/', clean_role_histories)
        # process reactions
        processData('guild_message_reactions', path + 'reactions', clean_guild_message_reactions)
        # process channels
        processData('guild_channels', path + 'channels/_', clean_guild_channels)
        processData('guild_channel_histories', path + 'channels/', clean_guild_channel_histories)
        print(f'Data for guild {guild_id} processed')

    cur.close()
    print("Done")
