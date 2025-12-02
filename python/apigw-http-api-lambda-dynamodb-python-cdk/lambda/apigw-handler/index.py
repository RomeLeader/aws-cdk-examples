# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries
patch_all()

import boto3
import os
import json
import logging
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    table = os.environ.get("TABLE_NAME")
    
    # Log request context for security audit
    request_context = event.get("requestContext", {})
    logger.info(json.dumps({
        "message": "Request received",
        "requestId": context.request_id,
        "sourceIp": request_context.get("identity", {}).get("sourceIp"),
        "userAgent": request_context.get("identity", {}).get("userAgent"),
        "requestTime": request_context.get("requestTime"),
        "httpMethod": request_context.get("httpMethod"),
        "path": request_context.get("path"),
    }))
    
    try:
        if event["body"]:
            item = json.loads(event["body"])
            logger.info(json.dumps({"message": "Processing payload", "item": item}))
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
        else:
            logger.info(json.dumps({"message": "Request without payload, using default data"}))
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": str(uuid.uuid4())},
                },
            )
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
    except Exception as e:
        logger.error(json.dumps({
            "message": "Error processing request",
            "error": str(e),
            "requestId": context.request_id,
        }))
        raise
