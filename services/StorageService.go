package services

import (
	"bqToFtp/helpers"
	"bqToFtp/models"
	"cloud.google.com/go/storage"
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
)

type IStorageService interface {
	FallbackStoreFile(name string, src []byte) (err error)
	GetQuery() string
}

type storageService struct {
	IStorageService
	query             string
	bucketQueryObject *storage.ObjectHandle
	latency           int
	minuteDelta       int
	fallbackBucket    *storage.BucketHandle
}

/*
Load the queryFilePath file and format the queryFilePath
*/
func NewStorageService(configService helpers.IConfigService) *storageService {
	this := &storageService{}

	query := configService.GetEnvVar(models.QUERY_FILE_PATH)
	minuteDeltaEnvVar := configService.GetEnvVar(models.MINUTE_DELTA)
	if query == "" || minuteDeltaEnvVar == "" {
		log.Fatalf("Error reading environment variables. Here the known variables: queryFilePath %q, minuteDelta %q", query, minuteDeltaEnvVar)
	}

	ctx := context.Background()

	clients, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal("Impossible to connect to storage client")
	}

	if !strings.HasPrefix(query, "gs://") {
		log.Fatalf("Error reading queryFilePath environment variables. No linked to a GCP Bucket file %q", query)
	}
	bucketName, pathName := extractBucketPath(query)
	bucketQueryObject := clients.Bucket(bucketName).Object(pathName)

	if isForceReload(configService.GetEnvVar(models.FORCE_RELOAD)) {
		this.bucketQueryObject = bucketQueryObject
	} else {
		this.query = loadQuery(bucketQueryObject)
	}

	latencyEnvVar := configService.GetEnvVar(models.LATENCY)
	if latencyEnvVar != "" {
		this.latency, err = strconv.Atoi(latencyEnvVar)
		if err != nil {
			log.Fatalf("Impossible to parse the latency %q", latencyEnvVar)
		}
	}

	this.minuteDelta, err = strconv.Atoi(minuteDeltaEnvVar)
	if err != nil {
		log.Fatalf("Impossible to parse the minute delta %q", minuteDeltaEnvVar)
	}

	//Load the fallback bucket
	if fallbackBucket := configService.GetEnvVar(models.FALLBACK_BUCKET); fallbackBucket != "" {
		bucketName, _ = extractBucketPath(fallbackBucket)
		this.fallbackBucket = clients.Bucket(bucketName)
	}

	return this
}

/*
Load the query string from the bucket. Fatal is something failed, it's the core feature of this app.
*/
func loadQuery(bucketQueryObject *storage.ObjectHandle) string {
	ctx := context.Background()
	objectReader, err := bucketQueryObject.NewReader(ctx)
	if err != nil {
		log.Fatalf("Impossible to find the queryFilePath file %q", bucketQueryObject.BucketName()+"/"+bucketQueryObject.ObjectName())
	}

	content, err := ioutil.ReadAll(objectReader)
	if err != nil {
		log.Fatalf("Impossible to read the queryFilePath file %q", bucketQueryObject.BucketName()+"/"+bucketQueryObject.ObjectName())
	}
	return string(content)
}

/*
 Return true is the FORCE_RELOAD param is set to TRUE (any case) or to 1
*/
func isForceReload(forceReload string) bool {
	if forceReload != "" && (strings.ToUpper(forceReload) == "TRUE" || strings.ToUpper(forceReload) == "1") {
		return true
	}
	return false
}

func extractBucketPath(query string) (bucketName string, path string) {
	cleanQuery := strings.ReplaceAll(query, "gs://", "")
	querySplitted := strings.Split(cleanQuery, "/")
	bucketName = querySplitted[0]
	path = strings.Join(querySplitted[1:], "/")
	return
}

/*
Load the query is not existing in the object
Replace the START_TIMESTAMP and END_TIMESTAMP in the original Query.
END value is calculated by taking the current minute of the execution (seconds at 0) and by subtracting the LATENCY var env value
START value is calculated by taking END value and by subtracting the MINUTE_DELTA var env value
*/
func (this *storageService) formatQuery() string {
	query := this.query
	//Test if the query if empty. If yes, this means a force reload
	if query == "" {
		query = loadQuery(this.bucketQueryObject)
	}

	//Smoking Gopher developer. WTF ??? why formating date on 2006-01-02 15:04:05 ??????
	format := "2006-01-02 15:04:05"

	now := time.Now()
	endDate := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, now.Location())
	endDate = endDate.Add(-time.Duration(this.latency) * time.Minute)
	startDate := endDate.Add(-time.Duration(this.minuteDelta) * time.Minute)
	return strings.ReplaceAll(strings.ReplaceAll(query, "START_TIMESTAMP", startDate.Format(format)), "END_TIMESTAMP", endDate.Format(format))
}

func (this *storageService) GetQuery() string {
	return this.formatQuery()
}

/*
Store the file in the fallback bucket in case of ftp error
*/
func (this *storageService) FallbackStoreFile(name string, src []byte) (err error) {
	if this.fallbackBucket == nil {
		log.Error("No fallback bucket defined or available. Impossible to save file")
		return errors.New("no fallback bucket defined")
	}
	ctx := context.Background()
	writer := this.fallbackBucket.Object(name).NewWriter(ctx)
	defer writer.Close()
	_, err = writer.Write(src)
	return
}
