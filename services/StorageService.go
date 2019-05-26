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
	StoreFile(name string, src []byte) (err error)
	GetQuery() string
}

type storageService struct {
	IStorageService
	query          string
	fallbackBucket *storage.BucketHandle
}

/*
Load the queryFilePath file and format the queryFilePath
*/
func NewStorageService(configService helpers.IConfigService) *storageService {
	storageService := &storageService{}

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
	objectReader, err := clients.Bucket(bucketName).Object(pathName).NewReader(ctx)
	if err != nil {
		log.Fatalf("Impossible to find the queryFilePath file %q", query)
	}

	content, err := ioutil.ReadAll(objectReader)
	if err != nil {
		log.Fatalf("Impossible to read the queryFilePath file %q", query)
	}

	latency := 0
	latencyEnvVar := configService.GetEnvVar(models.LATENCY)
	if latencyEnvVar != "" {
		latency, err = strconv.Atoi(latencyEnvVar)
		if err != nil {
			log.Fatalf("Impossible to parse the latency %q", latencyEnvVar)
		}
	}

	minuteDelta, err := strconv.Atoi(minuteDeltaEnvVar)
	if err != nil {
		log.Fatalf("Impossible to parse the minute delta %q", minuteDeltaEnvVar)
	}

	storageService.query = formatQuery(string(content), latency, minuteDelta)

	//Load the fallback bucket
	if fallbackBucket := configService.GetEnvVar(models.FALLBACK_BUCKET); fallbackBucket != "" {
		bucketName, _ = extractBucketPath(fallbackBucket)
		storageService.fallbackBucket = clients.Bucket(bucketName)
	}

	return storageService
}

func extractBucketPath(query string) (bucketName string, path string) {
	cleanQuery := strings.ReplaceAll(query, "gs://", "")
	querySplitted := strings.Split(cleanQuery, "/")
	bucketName = querySplitted[0]
	path = strings.Join(querySplitted[1:], "/")
	return
}

/*
Replace the START_TIMESTAMP and END_TIMESTAMP in the original Query.
END value is calculated by taking the current minute of the execution (seconds at 0) and by subtracting the LATENCY var env value
START value is calculated by taking END value and by subtracting the MINUTE_DELTA var env value
*/
func formatQuery(originalQuery string, latency int, minuteDelta int) string {
	//Smoking Gopher developer. WTF ??? why formating date on 2006-01-02 15:04:05 ??????
	format := "2006-01-02 15:04:05"

	now := time.Now()
	endDate := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, now.Location())
	endDate = endDate.Add(-time.Duration(latency) * time.Minute)
	startDate := endDate.Add(-time.Duration(minuteDelta) * time.Minute)
	return strings.ReplaceAll(strings.ReplaceAll(originalQuery, "START_TIMESTAMP", startDate.Format(format)), "END_TIMESTAMP", endDate.Format(format))
}

func (storageService *storageService) GetQuery() string {
	return storageService.query
}

func (storageService *storageService) StoreFile(name string, src []byte) (err error) {
	if storageService.fallbackBucket == nil {
		log.Error("No fallback bucket defined or available. Impossible to save file")
		return errors.New("no fallback bucket defined")
	}
	ctx := context.Background()
	writer := storageService.fallbackBucket.Object(name).NewWriter(ctx)
	defer writer.Close()
	_, err = writer.Write(src)
	return
}
