package main

import (
	"os"
	"time"

	"github.com/fsouza/go-dockerclient"
	"github.com/gliderlabs/logspout/router"
)

// NOTE: this main() function is not included in the compiled Docker
// container application. The program below is just a test program to
// verify the code in modules.go which gets packaged with gliderlabs/logspout.
func main() {
	os.Setenv("AWS_ACCESS_KEY", "...")
	os.Setenv("AWS_SECRET_KEY", "...")
	os.Setenv("AWS_REGION", "us-east-1")
	os.Setenv("FLUSH_INTERVAL", "1") // flush after 1 second

	// --

	bucketID := "pressly-logs-test"
	address := bucketID //+ "/logs"

	s3a, err := NewS3Adapter(&router.Route{
		Adapter: "s3",
		Address: address,
	})
	if err != nil {
		panic(err)
	}

	// Some test data
	go func(a *S3Adapter) {
		entries := []logEntry{
			{"api", "amsg1"},
			{"api", "amsg2"},
			{"api", "amsg3"},
			{"api", "amsg4"},
			{"api", "amsg5"},
			{"feed", "fmsg1"},
			{"feed", "fmsg2"},
			{"feed", "fmsg3"},
			{"feed", "fmsg4"},
			{"feed", "fmsg5"},
		}

		streamCh := make(chan *router.Message)
		go a.Stream(streamCh)

		for _, e := range entries {
			go func(e logEntry) {
				streamCh <- &router.Message{
					Container: &docker.Container{ID: "123", Name: e.Container},
					Data:      e.Message,
					Time:      time.Now(),
				}
			}(e)
		}
	}(s3a.(*S3Adapter))

	// block, wait for ctrl+c
	for {
	}
}
