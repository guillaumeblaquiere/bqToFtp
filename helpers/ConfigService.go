package helpers

import (
	"context"
	"github.com/GoogleCloudPlatform/berglas/pkg/berglas"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
)

type EnvVarEnum string

var berglasPrefix = "berglas://"

type IConfigService interface {
	GetEnvVar(enum EnvVarEnum) string
}

type BergasOrOsEnvVarConfigService struct {
}

// For berglas var env, Service Account need roles/storage.objectViewer and roles/cloudkms.cryptoKeyDecrypter
func (o *BergasOrOsEnvVarConfigService) GetEnvVar(enum EnvVarEnum) string {
	varEnv := os.Getenv(string(enum))
	if strings.HasPrefix(varEnv, berglasPrefix) {

		ctx := context.Background()

		// This higher-level API parses the secret reference at the specified
		// environment variable, downloads and decrypts the secret, and replaces the
		// contents of the given environment variable with the secret result.
		if err := berglas.Replace(ctx, string(enum)); err != nil {
			log.Fatal(err)
		}
		varEnv = os.Getenv(string(enum))
	}
	return varEnv
}
