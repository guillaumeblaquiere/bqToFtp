package services

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func Test_formatQuery(t *testing.T) {
	type args struct {
		originalQuery string
		latency       int
		minuteDelta   int
	}
	tests := []struct {
		name      string
		args      args
		wantFunc  func(string) (interface{}, interface{})
		wantStart interface{}
		wantEnd   interface{}
	}{
		{
			name: "Correct Date without latency",
			args: args{
				originalQuery: "START_TIMESTAMP|END_TIMESTAMP",
				latency:       0,
				minuteDelta:   15,
			},
			wantFunc: func(got string) (start interface{}, end interface{}) {
				split := strings.Split(got, "|")
				tStart, _ := time.Parse("2006-01-02 15:04:05", split[0])
				tEnd, _ := time.Parse("2006-01-02 15:04:05", split[1])
				now := time.Now()
				//Rewrite the date with the location of the date
				now = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, tStart.Location())
				return now.Sub(tStart).Seconds(), now.Sub(tEnd).Seconds()
			},
			wantStart: 15 * 60,
			wantEnd:   0,
		},
		{
			name: "Correct Date with latency",
			args: args{
				originalQuery: "START_TIMESTAMP|END_TIMESTAMP",
				latency:       10,
				minuteDelta:   15,
			},
			wantFunc: func(got string) (start interface{}, end interface{}) {
				split := strings.Split(got, "|")
				tStart, _ := time.Parse("2006-01-02 15:04:05", split[0])
				tEnd, _ := time.Parse("2006-01-02 15:04:05", split[1])
				now := time.Now()
				//Rewrite the date with the location of the date
				now = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, tStart.Location())
				return now.Sub(tStart).Seconds(), now.Sub(tEnd).Seconds()
			},
			wantStart: (10 + 15) * 60,
			wantEnd:   10 * 60,
		},
		{
			name: "Wrong token",
			args: args{
				originalQuery: "START_DATE|END_DATE",
				latency:       10,
				minuteDelta:   15,
			},
			wantFunc: func(got string) (start interface{}, end interface{}) {
				split := strings.Split(got, "|")
				return split[0], split[1]
			},
			wantStart: "START_DATE",
			wantEnd:   "END_DATE",
		},
		{
			name: "No queryFilePath",
			args: args{
				originalQuery: "",
				latency:       10,
				minuteDelta:   15,
			},
			wantFunc: func(got string) (start interface{}, end interface{}) {
				split := strings.Split(got, "|")
				return split[0], split[0]
			},
			wantStart: "",
			wantEnd:   "",
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatQuery(tt.args.originalQuery, tt.args.latency, tt.args.minuteDelta)
			start, end := tt.wantFunc(got)
			if !assert.EqualValues(t, start, tt.wantStart) {
				t.Errorf("formatQuery() startValue = %v, want %v", start, tt.wantStart)
			}
			if !assert.EqualValues(t, end, tt.wantEnd) {
				t.Errorf("formatQuery() endValue = %v, want %v", end, tt.wantEnd)
			}
		})
	}
}

func Test_extractBucketPath(t *testing.T) {
	type args struct {
		queryFilePath string
	}
	tests := []struct {
		name           string
		args           args
		wantBucketName string
		wantPath       string
	}{
		{
			name: "Correct bucket and path parsing",
			args: args{
				queryFilePath: "gs://my-bucket/path/to/file/query.sql",
			},
			wantBucketName: "my-bucket",
			wantPath:       "path/to/file/query.sql",
		},
		{
			name: "Correct bucket and only file",
			args: args{
				queryFilePath: "gs://my-bucket/query.sql",
			},
			wantBucketName: "my-bucket",
			wantPath:       "query.sql",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBucketName, gotPath := extractBucketPath(tt.args.queryFilePath)
			if gotBucketName != tt.wantBucketName {
				t.Errorf("extractBucketPath() gotBucketName = %v, want %v", gotBucketName, tt.wantBucketName)
			}
			if gotPath != tt.wantPath {
				t.Errorf("extractBucketPath() gotPath = %v, want %v", gotPath, tt.wantPath)
			}
		})
	}
}
