package main

import (
	"bqToFtp/controllers"
	"bqToFtp/helpers"
	"bqToFtp/services"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
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

	//Set the debug level or not
	setDebugLogLevel()

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), router))
}

func InitializeRouter() *mux.Router {
	// StrictSlash is true => redirect /cars/ to /cars
	router := mux.NewRouter().StrictSlash(true)

	//Init Controllers
	configService := &helpers.BergasOrOsEnvVarConfigService{}
	bigqueryService := services.NewBigQueryService(configService)
	ftpService := services.NewFtpService(configService)
	bqToFtpController := controllers.NewBqToFtpController(configService,bigqueryService, ftpService)

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