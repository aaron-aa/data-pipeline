# Copyright (c) 2015 App Annie Inc. All rights reserved.
import boto3


class S3Exception(Exception):
    pass


def connection():
    return boto3.resource('s3')


def put(bucket, key, data, acl_str="bucket-owner-full-control"):
    try:
        obj = connection().Object(bucket, key)
        obj.put(Body=data)
        obj.Acl().put(ACL=acl_str)
    except Exception as ex:
        raise S3Exception(ex)
