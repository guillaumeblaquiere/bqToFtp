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
func (this *BergasOrOsEnvVarConfigService) GetEnvVar(enum EnvVarEnum) string {
	varEnv := os.Getenv(string(enum))
	if strings.HasPrefix(varEnv, berglasPrefix) {

		ctx := context.Background()

		value, err := berglas.Resolve(ctx, varEnv)
		if err != nil {
			log.Fatal(err)
		}
		varEnv = string(value)
	}
	return varEnv
}
