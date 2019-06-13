package models

import (
	"bqToFtp/helpers"
)

const (
	QUERY_FILE_PATH helpers.EnvVarEnum = "QUERY_FILE_PATH"
	FORCE_RELOAD    helpers.EnvVarEnum = "FORCE_RELOAD"
	HEADER          helpers.EnvVarEnum = "HEADER"
	GCP_PROJECT     helpers.EnvVarEnum = "GCP_PROJECT"
	SEPARATOR       helpers.EnvVarEnum = "SEPARATOR"
	FILE_PREFIX     helpers.EnvVarEnum = "FILE_PREFIX"
	MINUTE_DELTA    helpers.EnvVarEnum = "MINUTE_DELTA"
	LATENCY         helpers.EnvVarEnum = "LATENCY"

	FTP_PATH        helpers.EnvVarEnum = "FTP_PATH"
	FTP_SERVER      helpers.EnvVarEnum = "FTP_SERVER"
	FTP_LOGIN       helpers.EnvVarEnum = "FTP_LOGIN"
	FTP_PASSWORD    helpers.EnvVarEnum = "FTP_PASSWORD"
	FALLBACK_BUCKET helpers.EnvVarEnum = "FALLBACK_BUCKET"
)
