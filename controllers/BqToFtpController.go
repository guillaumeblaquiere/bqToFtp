package controllers

import (
	"bqToFtp/helpers"
	"bqToFtp/models"
	"bqToFtp/services"
	"bytes"
	"cloud.google.com/go/bigquery"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"net/http"
	"strconv"
	"time"
)

/*
Allow to handle all the process message controller with and Handle function to process the API call
*/
type IBqToFtpController interface {
	Handle(w http.ResponseWriter, r *http.Request)
}

type bqToFtpController struct {
	IBqToFtpController
	configService   helpers.IConfigService
	bigQueryService services.IBigQueryService
	ftpService      services.IFTPService
	storageService  services.IStorageService
	withHeader      bool
	separator       []byte
	filePrefix      string
	timeFormat      string
}

/*
Factory which create an handler with a Parser.
Have to be instantiate with each parser
*/
func NewBqToFtpController(configService helpers.IConfigService, bigQueryService services.IBigQueryService, ftpService services.IFTPService, storageService services.IStorageService) *bqToFtpController {
	bqToFtpController := &bqToFtpController{}
	bqToFtpController.configService = configService
	bqToFtpController.bigQueryService = bigQueryService
	bqToFtpController.ftpService = ftpService
	bqToFtpController.storageService = storageService
	bqToFtpController.filePrefix = configService.GetEnvVar(models.FILE_PREFIX)
	var err error
	bqToFtpController.withHeader, err = strconv.ParseBool(configService.GetEnvVar(models.HEADER))
	if err != nil {
		log.Errorf("Impossible to convert to Boolean the HEADER parameter %q. Header is set to FALSE", configService.GetEnvVar(models.HEADER))
	}

	separator := configService.GetEnvVar(models.SEPARATOR)
	bqToFtpController.separator = []byte(separator)

	if separator == "" {
		bqToFtpController.separator = []byte(",")
	}
	bqToFtpController.timeFormat = "20060102150405"
	return bqToFtpController

}

/*
Apply a generic handler to the instantiated parser
*/
func (controller *bqToFtpController) Handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-type", "application/json;charset=UTF-8")

	//Read the query
	iter, err := controller.bigQueryService.Read(controller.storageService.GetQuery())
	if err != nil {
		log.Errorf("Error in BQ request %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	//Make a byteBuffer in memory
	fileInMemory, _ := createFileInMemory(controller.withHeader, controller.separator, iter)

	//Push the file to FTP
	//create the fileName
	fileName := controller.filePrefix + time.Now().Format(controller.timeFormat) + ".csv"

	if err = controller.sendFile(fileName, fileInMemory); err != nil {
		log.Errorf("Impossible to send the file with error %v\n Try to save file in fallback bucket", err)
		//save in fallback
		if err = controller.storageService.StoreFile(fileName, fileInMemory); err != nil {
			log.Errorf("Impossible to file in fallback bucket with error %v.here the full file content \n%q", err, string(fileInMemory))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (controller *bqToFtpController) sendFile(fileName string, fileInMemory []byte) error {
	numberOfError := 0
	for {
		err := controller.ftpService.Send(fileName, bytes.NewReader(fileInMemory))
		if err != nil {
			numberOfError++
			if numberOfError >= 3 {
				return errors.New("multiple ftp send attempt in error. ftp not reachable")
			}
			log.Warningf("Error while sending the file with error %v. Perform a retry", err)
		} else {
			//Correct send by ftp
			return nil
		}
	}
}

var lineSeparatorByte = []byte("\n")

func createFileInMemory(header bool, separator []byte, rowIterator services.IRowIterator) (fileInMemory []byte, err error) {
	buffer := bytes.Buffer{}

	//Write the Header if set to true

	if header {
		for i, schemaField := range rowIterator.GetSchema() {
			buffer.Write([]byte(schemaField.Name))
			//don't write the last separator
			if i != (len(rowIterator.GetSchema()) - 1) {
				buffer.Write(separator)
			}
		}
		buffer.Write(lineSeparatorByte)
	}

	//Loop on row.
	for {
		var values []bigquery.Value
		err := rowIterator.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			//should never occur !
		}
		for i, value := range values {
			buffer.Write([]byte(fmt.Sprint(value)))
			//don't write the last separator
			if i != (len(values) - 1) {
				buffer.Write(separator)
			}
		}
		buffer.Write(lineSeparatorByte)
	}
	fileInMemory = buffer.Bytes()
	return
}
