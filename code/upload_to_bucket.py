import logging
import boto3
import os
from botocore.exceptions import ClientError


def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """
    # If the bucket is already created
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    if bucket_name in buckets:
        print("Bucket {} already created".format(bucket_name))
        return False

    # Else create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
            print("Created bucket {} successfully".format(bucket_name))
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_file_to_s3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_file(folder_path, bucket_name):
    file_names = os.listdir(folder_path)
    
    for filename in file_names:
        file_path = os.path.join(folder_path,filename)
        upload_file_to_s3(file_name=file_path,bucket=bucket_name)
        print("Uploaded file {} to bucket {}".format(filename,bucket_name))
    print("Upload files to bucket {} successfully".format(bucket_name))
    

if __name__ == "__main__":
    # raw_receipts_path = "../data/email"
    # raw_bucket_name = "grab-receipts"
    # raw_bucket_region = "ap-southeast-2"
    # create_bucket(raw_bucket_name,raw_bucket_region)
    # upload_file(folder_path=raw_receipts_path, bucket_name=raw_bucket_name)

    processed_data_path = "../data/data"
    processed_bucket_name = "grab-transaction"
    processed_bucket_region = "ap-southeast-2"
    create_bucket(processed_bucket_name,processed_bucket_region)
    upload_file(folder_path=processed_data_path, bucket_name=processed_bucket_name)

