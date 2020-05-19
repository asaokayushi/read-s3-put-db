#　s3からファイル名を取得してDynamoDBに保存するコード

import boto3
import time

bucket_name = 'asaokatestbucket'  # S3バケット名
S3KeyPrefix = 'イラスト/'
table_name = 'kireiwatch'  # DynamoDBテーブル名

#名を取り出して、ファイル名のリストを取得する
def load_data(bucket_name, S3KeyPrefix):
    # keys ファイル名のリスト型を返す

    s3 = boto3.resource('s3')  # S3使いますよー
    bucket = s3.Bucket(bucket_name)
    objs = bucket.meta.client.list_objects_v2(Bucket=bucket.name, Prefix=S3KeyPrefix)
    keys = []
    print(objs.get('Contents'))
    print("データ読み込み")

    for key in objs.get('Contents'):
        key = key.get('Key')
        print(key)
        s3obj = s3.Object(bucket_name, key).get()
        keys.append(key)

    return keys


def put_data(table_name,keys):

    dynamo = boto3.resource('dynamodb',region_name='ap-northeast-1')  # DynamoDB使いますよー
    dynamo_table = dynamo.Table(table_name)  # このテーブル操作しますよ

    for key in keys:
        print("DynamoDB書き込み")
        item = {
            "EquipmentId": key,
            "Timestamp": 5467808,
            "value": int(time.time())

        }
        dynamo_table.put_item(Item=item)  # DynamoDB保存


keys = load_data(bucket_name, S3KeyPrefix)

print(keys)

put_data(table_name,keys)
