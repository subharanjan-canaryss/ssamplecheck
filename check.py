import boto3
import json
import os
from botocore.exceptions import ClientError
import uuid
from time import time
from uuid import uuid4

 

 

app_url = os.environ['APP_URL']
ddb = boto3.client('dynamodb')
sqs_client=boto3.client("sqs")

#converted_url=[]
def create_short_url(message):

    short_url_length = os.environ['SHORT_URL_LENGTH']

    try:
        short_id = str(uuid.uuid4())[0:int(short_url_length)]
        #print(short_id)

        try:
            if 'Item' in ddb.get_item(Key={'short_id': {'S': short_id}},TableName=os.environ['TABLE_NAME']):
                    print(short_id+" is already present")
                    short_id = str(uuid.uuid4())[0:int(short_url_length)]
        except Exception as e:
            print("Exception occured while getting the docId from the DDB due to "+str(e))

        short_url=app_url+'/'+short_id
        timestamp=int(time())
        ttl_value = expiry_date()
        doc_data = json.loads(message)

        response = ddb.put_item(
            TableName=os.environ['TABLE_NAME'],
            Item={
                'short_id':{'S': short_id},
                'created_at':{'N': str(timestamp)},
                'short_url':{'S': short_url},
                'docID':{'S': doc_data['docID']},
                'ttl_value':{'N': str(ttl_value)}
                }
            )

        #print("Response added to the ddb table")

        #converted = {"docID":doc_data['docID'],"short_url"}
        doc_data['short_url'] = short_url
        doc_data.pop('timestamp', None)

        return {"Id":str(uuid4()), "MessageBody": json.dumps(doc_data)}

    except Exception as e:
        print(e)

 

def expiry_date():
    ttl_value_epoch = os.environ['TTL_VALUE']
    try:    
        expiry_time = int(time()) + int(ttl_value_epoch) #generate expiration date for the url based on the timestamp
        return expiry_time

    except Exception as e:
        # Send some context about this error to Lambda Logs
        print(e)
        # throw exception, do not handle. Lambda will make message visible again.
        raise e

 

 

def sendMessage(mssg, converted_url):
    sqs_batch_size = 10
    rec_sqs = os.environ['SQS_URL']
    messages_to_reprocess = []
    batch_failure_response = {}
    withretry = []

 

    maxBatchSize = int(sqs_batch_size)

    chunks = [converted_url[x:x+maxBatchSize] for x in range(0,len(converted_url),maxBatchSize)]

    for chunk in chunks:
        print(chunk)
        key = "Retry"
        try:
            if not any(d.get('Retry', 'Retry') in d for d in chunk):
                sqs=sqs_client.send_message_batch(
                    QueueUrl = rec_sqs,
                    Entries = chunk
                )
                for item in chunk:
                    item.update( {'Retry':0})
                    print("Retry key added with value 0")
                    print(item)

                print("Batch message successfully pushed to 'sqs-from-ShortURL'")
            elif not any(d.get('Retry') <= 2 for d in chunk):
                print("Increase retry key with value 1")
                i = d.get('Retry')
                j  = i + 1
                item.update( {'Retry':j})
                print("Increase retry key by value 1")
                # Send to queue
                sqs=sqs_client.send_message_batch(
                    QueueUrl = rec_sqs,
                    Entries = chunk
                )
            else:
                print("More than 2 retries, sending to DLQ")
                # response = client.send_message_batch(QueueUrl='https://sqs.ap-south-1.amazonaws.com/541261225183/dlq-shortURL',Entries=chunk)

        except Exception as e:
            print(e)
            messages_to_reprocess.append({"itemIdentifier": mssg['messageId']})
    batch_failure_response["batchItemFailures"] = messages_to_reprocess
    # print(batch_failure_response)
    return batch_failure_response

 

 

def lambda_handler(event, context):

    print(event)
    converted_url = []
    try:
        for mssg in event['Records']:
            #print(mssg['body'])
            #print(type(mssg['body']))

 

            resp = create_short_url(mssg['body'])
            converted_url.append(resp)

    except Exception as e:
        print("failed "+str(e))

    #print("converted_url ",converted_url)

    sendMessage(mssg, converted_url)
