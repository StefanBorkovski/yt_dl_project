import json
import os

import boto3
from mypy_boto3_dynamodb.service_resource import Table
from .secrets import AWSCredentials


class S3Helper:    
    """ S3 helper class with basic functionalities that can be extended based on further needs. 
    """

    def __init__(self) -> boto3.client:
        self.s3_client = boto3.client('s3', 
        aws_access_key_id = AWSCredentials.AWS_ACCESS_KEY, 
        aws_secret_access_key = AWSCredentials.AWS_SECRET_KEY,
        region_name = AWSCredentials.AWS_REGION_NAME
        )

    def upload_file(self, filename: str, bucket: str, key: str, delete_filename: bool = True) -> None:
        # Upload file from filesystem
        self.s3_client.upload_file(
            Filename=filename,
            Bucket=bucket,
            Key=key,
        )
        # Delete the file if uploaded successfully
        if delete_filename == True:
            os.remove(filename)

    def upload_object(self, body: str, bucket: str, key: str) -> None:
        # Upload object from memory
        self.s3_client.put_object(
            Body=body,
            Bucket=bucket,
            Key=key,
        )

    def load_object(self, bucket: str, key: str) -> dict:
        # Parse the file type
        file_type = key.split('.')[-1]
        # Load object from S3
        obj = self.s3_client.get_object(
            Bucket=bucket,
            Key=key
        )
        if file_type == 'json':
            return json.loads(obj['Body'].read())
        else:
            raise NotImplementedError(f'Object conversion for filetype {file_type} is not implemented!')
        

class DynamoDBHelper:
    """ DynamoDB helper class with basic functionalities that can be extended based on further needs. 
    """

    def __init__(self, table_name: str) -> Table:
        dynamodb = boto3.resource('dynamodb', 
            aws_access_key_id = AWSCredentials.AWS_ACCESS_KEY, 
            aws_secret_access_key = AWSCredentials.AWS_SECRET_KEY,
            region_name = AWSCredentials.AWS_REGION_NAME
            )
        self.dynamodb_table = dynamodb.Table(table_name) 

    def import_item(self, item: dict):
        # Import one item to the table
        self.dynamodb_table.put_item(
            Item=item
        )

    def query_items(self, query_expressions: dict):
        # Query items from the table matching the query expressions 
        return self.dynamodb_table.query(
            **query_expressions
        )
    
    def query_all_table_items(self):
        # Query all items from table
        response = self.dynamodb_table.scan()
        items = response['Items']
        while 'LastEvaluatedKey' in response:
            response = self.dynamodb_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        return items