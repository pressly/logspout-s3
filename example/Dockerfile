FROM golang:1.9

ADD . /go/src/github.com/pressly/logspout-s3/example
WORKDIR /go/src/github.com/pressly/logspout-s3/example
RUN make build
RUN mv bin/ex-server /bin/

EXPOSE 3333

CMD ["/bin/ex-server"]
