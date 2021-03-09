import boto3
import os
import csv

# my credentials are removed for privacy
s3 = boto3.resource('s3', aws_access_key_id='access key',
                    aws_secret_access_key='secret key')

dyndb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id='access key',
                       aws_secret_access_key='secret key')

def create_s3_bucket():
    global s3

    try:
        s3.create_bucket(Bucket='kaizzhang-cloud-storage',
                         CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
    except:
        print("this may already exist")

    body = open(os.path.join(os.getcwd(), 'aws_portal.png'), 'rb')
    o = s3.Object('kaizzhang-cloud-storage', 'test').put(Body=body)
    s3.Object('kaizzhang-cloud-storage', 'test').Acl().put(ACL='public-read')


def create_db_table():
    global dyndb
    
    try:
        table = dyndb.create_table(TableName='DataTable', 
                                KeySchema=[
                                    {
                                        'AttributeName': 'PartitionKey',
                                        'KeyType': 'HASH'
                                    },
                                    {
                                        'AttributeName': 'RowKey',
                                        'KeyType': 'RANGE'
                                    }
                                ],
                                AttributeDefinitions=[
                                    {
                                        'AttributeName': 'PartitionKey',
                                        'AttributeType': 'S'
                                    },
                                    {
                                        'AttributeName': 'RowKey',
                                        'AttributeType': 'S'
                                    },
                                ],
                                ProvisionedThroughput={
                                    'ReadCapacityUnits': 5,
                                    'WriteCapacityUnits': 5
                                })
    except:
        table = dyndb.Table("DataTable")
        print("table may already exist")

    table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
    return table


def upload_blobs(table):
    global s3, dyndb

    with open(os.path.join(os.getcwd(), 'experiments.csv'), 'rt') as csvfile:
        csvf = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(csvf, None)
        for item in csvf:
            print(item)
            body = open(os.path.join(os.getcwd(), item[3]), 'rb')
            s3.Object('kaizzhang-cloud-storage', item[3]).put(Body=body)
            md = s3.Object('kaizzhang-cloud-storage',
                           item[3]).Acl().put(ACL='public-read')

            url = "https://s3-us-west-2.amazonaws.com/kaizzhang-cloud-storage/" + \
                item[3]
            metadata_item = {
                'PartitionKey': item[0], 'RowKey': item[1], 'description': item[4], 'date': item[2], 'url': url}

            try:
                table.put_item(Item=metadata_item)
            except:
                print("item may already be there or another failure")


def search_item(table):
    response = table.get_item(
        Key={
            'PartitionKey': 'experiment2',
            'RowKey': '2'
        }
    )

    item = response['Item']
    print(item)
    print(response)


if __name__ == '__main__':
    create_s3_bucket()
    dbTable = create_db_table()
    upload_blobs(dbTable)
    search_item(dbTable)
