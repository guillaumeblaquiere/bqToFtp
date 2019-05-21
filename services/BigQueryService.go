package services

import (
	"bqToFtp/helpers"
	"bqToFtp/models"
	"cloud.google.com/go/bigquery"
	"context"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
)

type IBigQueryService interface {
	Read() (iter *bigquery.RowIterator, err error)
}

type bigqueryService struct {
	IBigQueryService
	query *bigquery.Query
}

func NewBigQueryService(configService helpers.IConfigService) *bigqueryService {
	bigqueryService := &bigqueryService{}

	projectId := configService.GetEnvVar(models.GCP_PROJECT)
	query := configService.GetEnvVar(models.QUERY)
	minuteDeltaEnvVar := configService.GetEnvVar(models.MINUTE_DELTA)
	if projectId == "" || query == "" || minuteDeltaEnvVar == "" {
		log.Fatalf("Error reading environment variables. Here the known variables: project_id %q, query %q, minute_delta %q", projectId, query, minuteDeltaEnvVar)
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectId)
	//Connect the client to PubSub
	if err != nil {
		log.Fatalf("Impossible to connect to pubsub client for project %q", projectId)
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

	bigqueryService.query = client.Query(formatQuery(query, latency, minuteDelta))

	return bigqueryService
}

func (bigqueryService *bigqueryService) Read() (iter *bigquery.RowIterator, err error) {
	ctx := context.Background()
	return bigqueryService.query.Read(ctx)
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
