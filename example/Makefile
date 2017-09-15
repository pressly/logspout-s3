build:
	@mkdir -p ./bin
	GOGC=off go build -i -o ./bin/ex-server .

docker-image:
	docker build -t logspout-s3-ex-server .

docker-run:
	docker run --rm -p 3333:3333 logspout-s3-ex-server

docker-run-logspout:
	docker run --rm --name=logspout-s3 \
		-e 'BACKLOG=false' \
		-e 'AWS_ACCESS_KEY=YOUR_KEY' \
		-e 'AWS_SECRET_KEY=YOUR_SECRET_KEY' \
		-e 'AWS_REGION=us-east-1' \
		-e 'FLUSH_INTERVAL=2' \
		-e 'MAX_SINK_SIZE_MB=1' \
		--volume=/var/run/docker.sock:/var/run/docker.sock \
		--publish=127.0.0.1:8000:80 \
		pressly/logspout-s3 \
		s3://pressly-logs-test
