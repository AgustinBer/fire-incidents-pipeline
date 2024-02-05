import os
import boto3
import datetime
import requests
import pandas as pd
import logging
from io import StringIO
import hashlib

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

s3_client = boto3.client('s3')
BUCKET_NAME = os.getenv("LANDING_BUCKET")
API_LIMIT = os.getenv("API_LIMIT")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

def calculate_md5_checksum(data):
    """Calculate MD5 checksum for a dataframe."""
    return hashlib.md5(data.encode()).hexdigest()

def post_to_slack(message):
    slack_data = {'text': message}
    response = requests.post(
        SLACK_WEBHOOK_URL,
        json=slack_data,
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        logger.error(f"Failed to send message to Slack. Status Code: {response.status_code}")

def lambda_handler(event, context):
    try:
        url = f"https://data.sfgov.org/resource/wr8u-xric.csv?$limit={API_LIMIT}"

        response = requests.get(url)
        
        if response.status_code == 200:
            current_date = datetime.datetime.now().strftime('%Y-%m-%d')
            object_key = f'fire_incidents_data_{current_date}.csv'

            data = pd.read_csv(StringIO(response.content.decode('utf-8')))
            data['processing_date'] = current_date
            csv_string = data.to_csv(index=False)
            checksum = calculate_md5_checksum(csv_string)
            
            csv_buffer = StringIO(csv_string)
            
            s3_client.put_object(Bucket=BUCKET_NAME, Key=object_key, Body=csv_buffer.getvalue(), Metadata={'checksum': checksum})
            logger.info(f'Successfully uploaded {object_key} to S3 bucket {BUCKET_NAME}.')
        else:
            error_message = f'Failed to download the file. HTTP Status Code: {response.status_code}'
            logger.error(error_message)
            post_to_slack(error_message)
            return {
                'statusCode': response.status_code,
                'body': 'Failed to download the CSV file from the endpoint.'
            }
    except Exception as e:
        error_message = f'An error occurred: {str(e)}'
        logger.exception(error_message)
        post_to_slack(error_message)  # Send error notification to Slack
        return {
            'statusCode': 500,
            'body': 'An error occurred while processing the request.'
        }

    return {
        'statusCode': 200,
        'body': f'Successfully processed and uploaded the data to {BUCKET_NAME}.'
    }
