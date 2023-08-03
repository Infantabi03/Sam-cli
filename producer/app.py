import json
import boto3
from botocore.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

my_config = Config(
    region_name = 'us-west-2',
)

def producer(event, context):
    logger.info(event)
    sqs = boto3.client('sqs')
    queueUrl = 'https://sqs.ap-south-1.amazonaws.com/033509534972/test'

    Msg = {'Abisha': 'Hi'}
    s1 = json.dumps(Msg)


    data = sqs.send_message(  MessageBody= s1,
       QueueUrl = queueUrl,
       DelaySeconds = 0
       )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Producer",
                "data":data
            }
        ),
    }
