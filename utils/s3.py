# Copyright (c) 2015 App Annie Inc. All rights reserved.
import boto3
import botocore


class S3Exception(Exception):
    pass


def _s3():
    return boto3.resource('s3')


def put(bucket, key, data, acl_str="bucket-owner-full-control"):
    try:
        obj = _s3().Object(bucket, key)
        obj.put(Body=data)
        obj.Acl().put(ACL=acl_str)
    except Exception as ex:
        raise S3Exception(ex)


def rename(bucket, from_key, to_key):
    try:
        s3 = _s3()
        copy_source = {'Bucket': bucket, 'Key': from_key}
        if exist(bucket, from_key):
            s3.Object(bucket, to_key).copy_from(CopySource=copy_source)
            s3.Object(bucket, from_key).delete()
    except Exception as ex:
        raise S3Exception(ex)


def delete(bucket, key):
    try:
        s3 = _s3()
        if exist(bucket, key):
            s3.Object(bucket, key).delete()
    except Exception as ex:
        raise S3Exception(ex)


def exist(bucket, key):
    try:
        s3 = _s3()
        s3.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            # Something else has gone wrong.
            raise
    else:
        return True


def list(bucket, prefix):
    try:
        s3 = _s3()
        bucket = s3.Bucket(bucket)
        objects = bucket.objects.filter(Prefix=prefix)
        return [obj.key for obj in objects]
    except Exception as ex:
        raise S3Exception(ex)
