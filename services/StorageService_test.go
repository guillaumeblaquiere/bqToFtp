package services

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
)

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

func Test_storageService_formatQuery(t *testing.T) {
	type fields struct {
		IStorageService IStorageService
		query           string
		latency         int
		minuteDelta     int
		fallbackBucket  *storage.BucketHandle
	}
	tests := []struct {
		name      string
		fields    fields
		wantFunc  func(string) (interface{}, interface{})
		wantStart interface{}
		wantEnd   interface{}
	}{
		{
			name: "Correct Date without latency",
			fields: fields{
				query:       "START_TIMESTAMP|END_TIMESTAMP",
				latency:     0,
				minuteDelta: 15,
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
			fields: fields{
				query:       "START_TIMESTAMP|END_TIMESTAMP",
				latency:     10,
				minuteDelta: 15,
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
			fields: fields{
				query:       "START_DATE|END_DATE",
				latency:     10,
				minuteDelta: 15,
			},
			wantFunc: func(got string) (start interface{}, end interface{}) {
				split := strings.Split(got, "|")
				return split[0], split[1]
			},
			wantStart: "START_DATE",
			wantEnd:   "END_DATE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storageService := &storageService{
				IStorageService: tt.fields.IStorageService,
				query:           tt.fields.query,
				latency:         tt.fields.latency,
				minuteDelta:     tt.fields.minuteDelta,
				fallbackBucket:  tt.fields.fallbackBucket,
			}
			got := storageService.formatQuery()
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

func Test_isForceReload(t *testing.T) {
	type args struct {
		forceReload string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Empty param",
			args: args{
				forceReload: "",
			},
			want: false,
		},
		{
			name: "false param",
			args: args{
				forceReload: "false",
			},
			want: false,
		},
		{
			name: "False param",
			args: args{
				forceReload: "False",
			},
			want: false,
		},
		{
			name: "FALSE param",
			args: args{
				forceReload: "FALSE",
			},
			want: false,
		},
		{
			name: "2 param",
			args: args{
				forceReload: "2",
			},
			want: false,
		},
		{
			name: "0 param",
			args: args{
				forceReload: "0",
			},
			want: false,
		},
		{
			name: "true param",
			args: args{
				forceReload: "true",
			},
			want: true,
		},
		{
			name: "True param",
			args: args{
				forceReload: "True",
			},
			want: true,
		},
		{
			name: "TRUE param",
			args: args{
				forceReload: "TRUE",
			},
			want: true,
		},
		{
			name: "1 param",
			args: args{
				forceReload: "1",
			},
			want: true,
		},
		{
			name: "TRue param",
			args: args{
				forceReload: "TRue",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isForceReload(tt.args.forceReload); got != tt.want {
				t.Errorf("isForceReload() = %v, want %v", got, tt.want)
			}
		})
	}
}
