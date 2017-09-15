build:
	@echo "hi"

docker-image:
	export VERSION=s3 && docker build -t pressly/logspout-s3 .
