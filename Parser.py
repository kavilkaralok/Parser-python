import boto3
import requests
from dotenv import load_dotenv
import os
import json
from pymongo import MongoClient

# load dotenv
load_dotenv()

queue_url = os.getenv('QUEUE_URL')
omdb_url = os.getenv('OMDB_URL')
omdb_api_key = os.getenv('OMDB_API')
aws_region= os.getenv('AWS_REGION')
mongo_url = os.getenv('MONGO_URL')
db_name = os.getenv('DB_NAME')
col_name = os.getenv('COL_NAME')


# module initializing...
sqs = boto3.client('sqs', region_name=aws_region)
client = MongoClient(mongo_url)
db = client[db_name]
collection = db[col_name]


def process_messages():
    
    movie_data = []

    response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=2,
            WaitTimeSeconds=10
    )

    message = response.get("Messages", [])

    for mess in message:
        movie_title = mess.get("Body")
        request_id = mess.get("ReceiptHandle")

        print(f"Processing {movie_title} details")
        
        response = requests.get(omdb_url, params = {
            't':movie_title,
            'apikey':omdb_api_key
            })

        if response.status_code == 200:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=request_id)
            movie_data.append(response.json());

    for movie in movie_data:
        collection.insert_one(movie)



if __name__ == "__main__":
    process_messages()
