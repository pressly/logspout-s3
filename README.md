# Logspout-S3

Logspout-S3 is a container app that listens on the Docker bus and streams
application logs to Amazon S3 at some FLUSH_INTERVAL or MAX_SINK_SIZE_MB. It's
built as an adapter to github.com/gliderlabs/logspout.


## Usage

```shell
docker run -d --name=logspout-s3 \
	-e 'BACKLOG=false' \
	-e 'AWS_ACCESS_KEY=YOUR_KEY' \
	-e 'AWS_SECRET_KEY=YOUR_SECRET_KEY' \
	-e 'AWS_REGION=us-east-1' \
	-e 'FLUSH_INTERVAL=120' \
	-e 'MAX_SINK_SIZE_MB=16' \
	--volume=/var/run/docker.sock:/var/run/docker.sock \
	pressly/logspout-s3 \
	s3://pressly-logs-test
```

See the `example/` folder and Makefile to get started. It demonstrates a REST API
service with JSON structured logging output, and also the start command
for logspout-s3 which will stream those logs.

If you'd like to query the logs, check out https://aws.amazon.com/athena/ which
uses S3 as a backend for any kind of log data to query/visualize.


## Image run options

The container app supports a few environment variables as options:

* `AWS_ACCESS_KEY` : your AWS access key
* `AWS_SECRET_KEY` : your AWS secret key
* `AWS_REGION` : the AWS region where your S3 bucket resides
* `FLUSH_INTERVAL` : interval that collected logs are then uploaded to S3, as seconds (default: 120)
* `MAX_SINK_SIZE_MB` : max buffer size for log collection before sending to S3, as MB (default: 16)

as well, the container command where in above example its `s3://pressly-logs-test` can also
accept additional paths as the folder prefix, ie. `s3://pressly-logs-test/logs`


## Tips

Setup an IAM user with the S3 policy to only write access to your logging buckets, like:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:PutObject"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::pressly-logs-test",
                "arn:aws:s3:::pressly-logs-test/*"
            ]
        }
    ]
}
```


## LICENSE

MIT
