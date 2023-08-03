import json
import boto3
from botocore.config import Config
import logging
from botocore.exceptions import ClientError
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

my_config = Config(
    region_name = 'us-west-2',
)




credential = {}

# secret_name = "prod/postgresdb"
# region_name = "ap-south-1"

# # Create a Secrets Manager client
# session = boto3.session.Session()
# client = session.client(
# service_name='secretsmanager',
#         region_name=region_name
#     )

# try:
#     get_secret_value_response = client.get_secret_value(
#             SecretId=secret_name
#         )
# except ClientError as e:
#         # For a list of exceptions thrown, see
#         # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
#         raise e

#     # Decrypts secret using the associated KMS key.
# secret = get_secret_value_response['SecretString']

#     # Your code goes here.
# secret = json.loads(get_secret_value_response['SecretString'])
    
# credential['username'] = secret['username']
# credential['password'] = secret['password']
# credential['host'] = "db-instance.c503gfuzpork.ap-south-1.rds.amazonaws.com"
# credential['db'] = "test_db"



credential={}
credential['host']= "db-instance.c503gfuzpork.ap-south-1.rds.amazonaws.com"
credential['port']=5432
credential['username']="postgres"
credential['password']= ""
credential['db']= "test_db"





def receiver(event, context):
    logger.info(credential)
    connections = psycopg2.connect(host=credential['host'],port=credential['port'],user=credential['username'], password=credential['password'], database=credential['db'],connect_timeout=120,sslrootcert="rds-ca-2019")
    # logger.info(event)
    cur=connections.cursor()

        # Execute a command: this creates a new table
    # cur.execute("""
    #         CREATE TABLE test (
    #             id serial PRIMARY KEY,
    #             num integer,
    #             data text)
    #         """)
    cur.execute("SELECT version();")

    # logger.info(cur.fetchone())


    sqs = boto3.client('sqs')
    queueUrl = 'https://sqs.ap-south-1.amazonaws.com/033509534972/test'
  
    data = sqs.receive_message(
        QueueUrl = queueUrl,
       MaxNumberOfMessages = 1,
    VisibilityTimeout = 10,
    WaitTimeSeconds = 0,
    )

    logger.info("Output")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Receiver",
                "data": data['Messages'][0]['Body'],
                "db_data": cur.fetchone()
            }
        ),
    }


