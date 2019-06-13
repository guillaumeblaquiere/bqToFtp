package services

import (
	"bqToFtp/helpers"
	"bqToFtp/models"
	"cloud.google.com/go/bigquery"
	"context"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

type IBigQueryService interface {
	Read(query string) (iter *RowIteratorWrapper, err error)
}

type bigqueryService struct {
	IBigQueryService
	client *bigquery.Client
}

func NewBigQueryService(configService helpers.IConfigService) *bigqueryService {
	this := &bigqueryService{}

	projectId := configService.GetEnvVar(models.GCP_PROJECT)
	if projectId == "" {
		log.Fatalf("Error reading environment variables. Here the known variables: project_id %q", projectId)
	}

	var err error
	ctx := context.Background()
	this.client, err = bigquery.NewClient(ctx, projectId)
	//Connect the client to PubSub
	if err != nil {
		log.Fatalf("Impossible to connect to pubsub client for project %q", projectId)
	}

	return this
}

/*Wrap the row iterator for allowing the testing*/
type IRowIterator interface {
	Next(dst interface{}) error
	PageInfo() *iterator.PageInfo
	GetSchema() bigquery.Schema
}

//Implementation for bigquery, inherit from bigquery.RowIterator
type RowIteratorWrapper struct {
	*bigquery.RowIterator
}

func (iter *RowIteratorWrapper) GetSchema() bigquery.Schema {
	return iter.Schema
}

func (this *bigqueryService) Read(query string) (iter *RowIteratorWrapper, err error) {
	ctx := context.Background()
	iterBigquery, err := this.client.Query(query).Read(ctx)
	iter = &RowIteratorWrapper{iterBigquery}
	return
}
