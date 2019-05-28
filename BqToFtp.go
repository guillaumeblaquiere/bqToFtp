package main

import (
	"bqToFtp/controllers"
	"bqToFtp/helpers"
	"bqToFtp/services"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	joonix "github.com/joonix/log"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
)

func main() {
	router := InitializeRouter()
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.SetFormatter(joonix.NewFormatter())
	//Set the debug level or not
	setDebugLogLevel()

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), router))
}

func InitializeRouter() *mux.Router {
	// StrictSlash is true => redirect /cars/ to /cars
	router := mux.NewRouter().StrictSlash(true)

	//Init Controllers
	configService := &helpers.BergasOrOsEnvVarConfigService{}

	//Load concurrently
	bigqueryCHan := make(chan services.IBigQueryService)
	ftpCHan := make(chan services.IFTPService)
	storageCHan := make(chan services.IStorageService)

	go func() { bigqueryCHan <- services.NewBigQueryService(configService) }()
	go func() { storageCHan <- services.NewStorageService(configService) }()
	go func() { ftpCHan <- services.NewFtpService(configService) }()

	bigqueryService := <-bigqueryCHan
	storageService := <-storageCHan
	ftpService := <-ftpCHan
	bqToFtpController := controllers.NewBqToFtpController(configService, bigqueryService, ftpService, storageService)

	router.Methods("GET").Path("/").HandlerFunc(bqToFtpController.Handle)
	return router
}

func setDebugLogLevel() {
	lvl := flag.String("level", log.DebugLevel.String(), "log level")
	flag.Parse()

	level, err := log.ParseLevel(*lvl)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	log.SetLevel(level)
}
