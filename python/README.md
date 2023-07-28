# S3 access log roller

## Introduction

The script rolls up S3 access log files from this:

```
s3://bucket1/example.com/2022-07-01-05-15-43-42117B9A70B0F1BA
s3://bucket1/example.com/2022-07-01-05-21-46-66A49E4B82CEA312
s3://bucket1/example.com/2022-07-01-05-31-51-2F572D0BD05E48EF
s3://bucket1/example.com/2022-07-02-00-21-50-EA56CA33F44CFE0E
s3://bucket1/example.com/2022-07-02-00-41-28-C88B4724EC22EBB0
s3://bucket1/example.com/2022-07-02-05-13-29-E4E5C6CF6881DCA8
s3://bucket1/example.com/2022-07-03-01-24-22-C8FB35206F454BBA
s3://bucket1/example.com/2022-07-03-01-27-42-5AD4C461AB01E837
s3://bucket1/example.com/2022-07-03-05-36-30-2C5ECE485C48D3EB
```

To this:

```
s3://bucket1/example.com/rollup-2022-07-01-e26daa03.tgz
s3://bucket1/example.com/rollup-2022-07-02-f14dbc6d.tgz
s3://bucket1/example.com/rollup-2022-07-03-0ec9ad7a.tgz
```

Access logs of the same day (UTC) are compressed into one single tarball, and the original files are optionally deleted.

The script only processes logs from yesterday or earlier. For example, if today is July 5 (UTC), it only processes files from July 4 (UTC) and earlier dates.

## Architecture

The script runs in two modes: producer and worker.

- In **producer** mode, the script recursively searches for [access log files](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html#server-log-keyname-format) in designated S3 prefixes, generates tasks, and sends them to SQS.
- In **worker** mode, the script receives tasks from SQS, and does the heavy-lifting of downloading / compressing / uploading / deleting files. Because S3 charges more for cross-region traffic, it is suggested to run workers in the same AWS region as the S3 bucket to save money.

The script is designed to run as a Lambda function. An [EventBridge schedule](https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html) invokes the function in **producer** mode, which sends tasks to an SQS queue. When SQS receives new tasks, it invokes the function in **worker** mode. SQS automatically splits the tasks in batches, launching up multiple Lambda instances in parallel.

## Running locally

For testing and development purposes, you can also run the script manually on your local laptop instead of on Lambda.

To run in producer mode:

```bash
./main.py producer \
  --queue-name s3-rollup-us-west-2 \
  --s3-role arn:aws:sts::123456789012:role/s3-rollup-bucket-access \
  --prefixes s3://bucket1/ s3://bucket2/logs/ s3://bucket3/access-logs/example.com/
```

To run in worker mode:

```bash
./main.py worker \
  --queue-name s3-rollup-us-west-2 \
  --delete  # deletes original files after successfully uploading the tarball
```
