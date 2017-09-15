package main

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/gliderlabs/logspout/adapters/syslog"
	"github.com/gliderlabs/logspout/router"
	_ "github.com/gliderlabs/logspout/transports/tcp"
	_ "github.com/gliderlabs/logspout/transports/tls"
	_ "github.com/gliderlabs/logspout/transports/udp"
	"github.com/oklog/ulid"
	"golang.org/x/net/context"
)

var entropy io.Reader

func init() {
	t := time.Now()
	entropy = rand.New(rand.NewSource(t.UnixNano()))

	router.AdapterFactories.Register(NewS3Adapter, "s3")
}

func NewS3Adapter(route *router.Route) (router.LogAdapter, error) {
	s3Key := os.Getenv("AWS_ACCESS_KEY")
	s3Secret := os.Getenv("AWS_SECRET_KEY")

	s3Region := os.Getenv("AWS_REGION")
	if s3Region == "" {
		s3Region = "us-east-1"
	}

	// Default 2 minute log flushing interval to push logs to S3
	flushInterval := time.Duration(2 * time.Minute)
	if s := os.Getenv("FLUSH_INTERVAL"); s != "" {
		i, _ := strconv.ParseInt(s, 10, 64)
		if i > 0 {
			flushInterval = time.Duration(time.Duration(i) * time.Second)
		}
	}

	// Parse S3 bucket and storage path from route address
	paths := strings.Split(route.Address, "/")
	bucketID := paths[0]
	storePath := "/" + strings.Join(paths[1:], "/")
	if !strings.HasSuffix(storePath, "/") {
		storePath += "/"
	}

	// Auth and session setup for AWS-S3
	creds := credentials.NewStaticCredentials(s3Key, s3Secret, "")
	if _, err := creds.Get(); err != nil {
		return nil, err
	}

	awsConf := aws.NewConfig().WithRegion(s3Region).WithCredentials(creds)
	conn := s3.New(session.New(), awsConf)

	a := &S3Adapter{
		route:         route,
		bucketID:      bucketID,
		storePath:     storePath,
		conn:          conn,
		recordCh:      make(chan logEntry),
		logSink:       newLogSink(),
		flushInterval: flushInterval,
	}

	// Verify connection works with a simple request
	_, err := a.conn.PutObject(&s3.PutObjectInput{
		Bucket:      aws.String(a.bucketID),
		Key:         aws.String("ping"),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		return nil, err
	}

	return a, nil
}

type S3Adapter struct {
	route     *router.Route
	bucketID  string
	storePath string
	conn      *s3.S3

	recordCh      chan logEntry
	logSink       *logSink
	flushInterval time.Duration
}

type logEntry struct {
	Container string
	Message   string
}

// Stream sends log data to a connection
func (a *S3Adapter) Stream(logstream chan *router.Message) {
	go a.recorder(a.flushInterval)

	for message := range logstream {
		a.recordCh <- logEntry{
			Container: message.Container.Name,
			Message:   message.Data,
		}
	}
}

func (a *S3Adapter) recorder(d time.Duration) {
	ticker := time.NewTicker(d)
	for {
		select {

		case entry := <-a.recordCh:
			a.logSink.Push(entry)

		case <-ticker.C:
			// TODO: we could check error response, after X consequtive errors
			// we can stop processing, or at least stop for some period of time.
			go a.sendLogs()

		}
	}
}

func (a *S3Adapter) sendLogs() error {
	// Check if any entries to send and preempt if not
	a.logSink.Lock()
	if len(a.logSink.entries) == 0 {
		a.logSink.Unlock()
		return nil
	}

	// Create container index from sink entries
	logs := map[string][]string{}
	for _, e := range a.logSink.entries {
		logs[e.Container] = append(logs[e.Container], e.Message)
	}

	a.logSink.entries = a.logSink.entries[:0]
	a.logSink.Unlock()

	// computed key name becomes: /{storePath}/{containerName}/{unix-ts}-{ulid}.log
	t := time.Now().Unix()
	ulid := newULID()
	filename := fmt.Sprintf("%d-%s", t, ulid.String())

	for name, msgs := range logs {
		if len(msgs) == 0 {
			continue
		}

		key := fmt.Sprintf("%s%s/%s.log", a.storePath, name, filename)
		data := []byte(strings.Join(msgs, "\r\n"))

		// fmt.Println("sendLogs: write", key, data)

		params := &s3.PutObjectInput{
			Bucket:        aws.String(a.bucketID),
			Key:           aws.String(key),
			Body:          bytes.NewReader(data),
			ContentLength: aws.Int64(int64(len(data))),
			ContentType:   aws.String("text/plain"),
		}

		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, time.Duration(5*time.Minute))
		defer cancel()

		_, err := a.conn.PutObjectWithContext(ctx, params)
		if err != nil {
			// fmt.Println("sendLogs: error - ", err)
			return err
		}
	}

	return nil
}

type logSink struct {
	sync.Mutex
	entries []logEntry
}

func newLogSink() *logSink {
	return &logSink{
		entries: []logEntry{},
	}
}

func (s *logSink) Push(e logEntry) {
	s.Lock()
	s.entries = append(s.entries, e)
	s.Unlock()
}

func newULID() ulid.ULID {
	t := time.Now()
	return ulid.MustNew(ulid.Timestamp(t), entropy)
}
