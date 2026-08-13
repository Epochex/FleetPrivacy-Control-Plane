#!/bin/sh
set -eu

key_id="$(awslocal kms create-key --description 'FleetPrivacy local data key' --query KeyMetadata.KeyId --output text)"
awslocal kms create-alias --alias-name alias/fleetprivacy-data --target-key-id "$key_id"

awslocal s3api create-bucket \
  --bucket fleetprivacy-artifacts \
  --create-bucket-configuration LocationConstraint=eu-west-1
awslocal s3api put-public-access-block \
  --bucket fleetprivacy-artifacts \
  --public-access-block-configuration \
  'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true'
awslocal s3api put-bucket-encryption \
  --bucket fleetprivacy-artifacts \
  --server-side-encryption-configuration \
  "{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"aws:kms\",\"KMSMasterKeyID\":\"$key_id\"},\"BucketKeyEnabled\":true}]}"

dlq_url="$(awslocal sqs create-queue --queue-name fleetprivacy-requests-dlq --query QueueUrl --output text)"
dlq_arn="$(awslocal sqs get-queue-attributes --queue-url "$dlq_url" --attribute-names QueueArn --query Attributes.QueueArn --output text)"
awslocal sqs create-queue \
  --queue-name fleetprivacy-requests \
  --attributes "{\"VisibilityTimeout\":\"60\",\"ReceiveMessageWaitTimeSeconds\":\"20\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$dlq_arn\\\",\\\"maxReceiveCount\\\":5}\"}"
