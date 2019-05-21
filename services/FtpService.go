package services

import (
	"bqToFtp/helpers"
	"bqToFtp/models"
	ftp "github.com/secsy/goftp"
	log "github.com/sirupsen/logrus"
	"io"
	"strings"
)

type IFTPService interface {
	Send(name string, src io.Reader) (err error)
}

type ftpService struct {
	IBigQueryService
	config ftp.Config
	host   string
	path   string
}

func NewFtpService(configService helpers.IConfigService) *ftpService {
	ftpService := &ftpService{}

	projectId := configService.GetEnvVar(models.GCP_PROJECT)
	ftpService.host = configService.GetEnvVar(models.FTP_SERVER)
	if projectId == "" || ftpService.host == "" {
		log.Fatalf("Error reading environment variables. Here the known variables: project_id %s, ftp server %s", projectId, ftpService.host)
	}

	ftpService.config = ftp.Config{
		User:     configService.GetEnvVar(models.FTP_LOGIN),
		Password: configService.GetEnvVar(models.FTP_PASSWORD),
	}

	ftpService.path = formatFtpPath(configService.GetEnvVar(models.FTP_PATH))

	return ftpService
}

func formatFtpPath(path string) (formattedPath string) {

	formattedPath = path
	if !strings.HasSuffix(formattedPath, "/") {
		formattedPath += "/"
	}
	if !strings.HasPrefix(formattedPath, "/") {
		formattedPath = "/" + formattedPath
	}

	return
}

func (ftpService *ftpService) Send(name string, src io.Reader) (err error) {
	client, err := ftp.DialConfig(ftpService.config, ftpService.host)
	//Close the connection at the end
	defer client.Close()
	return client.Store(ftpService.path+name, src)
}
