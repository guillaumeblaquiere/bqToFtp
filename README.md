# Overview
BQ To Ftp allows to request data in BigQuery, to build a csv file and to send it to a FTP server.
It could be useful when small piece of data in BigQuery have to be transferred on existing infrastructure or deliver to customers

The process is the following:
 - The query is customized with the start and the end date.
 - The query is performed to BQ
 - The result is write in a csv file, as received without transformation. If Header is provided in parameter, it's added in the file
 - The file is pushed to FTP server. 3 retry performed before trying to save the file in the fallback bucket

The output file name is `<FILE_PREFIX><YYYYMMDDhhmmss>.csv`. Only File prefix is customizable

Secret encryption can be handled by Berglas. You can found the documentation here
https://github.com/GoogleCloudPlatform/berglas

## Deployment design

BQ to FTP is designed to be deployed on [Cloud Run][cloud-run] and
triggered periodically via [Cloud Scheduler][cloud-scheduler]

## Current limitations

The extract is done in memory, the total file size can be more than the memory size allowed for the app (2gb max).

The Ftp sever must be reachable on internet without source ip filtering. No yet support FTPs or sFTP connexion.

# Configuration

An empty configuration .env file exist. Fill in with this requirement

 - **QUERY_FILE_PATH**: Google storage path to Query sql file to load for performing the query in BigQuery
 - **FORCE_RELOAD**: Force to reload the query file from the storage on each invocation. Default: false if missing or different of _1_ or _true_ case insensitive.
 _Be careful_ the processing time will be longer but you can gain in flexibility (no new deployment needed for reloading the latest sql file)
 - **LATENCY**: The number of minute in past for calculating the endDate of the query from now. 0 if missing
 - **MINUTE_DELTA**: the number of minute in past for calculating the StartDate of the query from EndDate
 - **HEADER**: Set to true (or 1) to activate the header in the CSV file. Column names are those in the request
 - **GCP_PROJECT**: Project where the Topics are set up
 - **SEPARATOR**: value separator in the CSV file. Comma , by default
 - **FILE_PREFIX**: file name prefix. 

 - **FTP_SERVER**: Ftp server URL. _required_
 - **FTP_LOGIN**: Ftp login. Can be empty if no authentication
 - **FTP_PASSWORD**: Ftp login. Can be empty if no authentication. If set, Berglas security is recommended
 - **FTP_PATH**: ftp path where to put the file. In / if missing. Path must exists in FTP (no auto-create)
 - **FALLBACK_BUCKET**: Bucket to use in case of ftp sending error. Store in root path. Bucket must exists in FTP (no auto-create)

## Start and End date customization
The query can be customizable by providing a START_TIMESTAMP and END_TIMESTAMP keyword, in a clause WHERE and on a TIMESTAMP field type.
```
For example
SELECT .... WHERE dateInsert BETWEEN TIMESTAMP("START_TIMESTAMP") AND TIMESTAMP("END_TIMESTAMP")
```
The end is calculated by taking the current time and subtracting the latency parameter (in minute). The seconds are set to 0.
The start is calculated by taking end timestamp and subtracting the minute delta parameter (in minute).

The latency is present to request the data some minutes in the past, for being sure that the data are present at the query time.


## Berglas
Secret management with berglas is easier. To create a secret use

```
berglas create <berglas bucket>/json_auth 'MY SECRET' \
    --key projects/$(gcloud config list --format='value(core.project)')/locations/global/keyRings/berglas/cryptoKeys/berglas-key
```
Then, in your env var use this reference
```
berglas://<berglas bucket>/json_auth

```

# Local run

```
# Load env var or set it to your IDE (IntelliJ/Goland need a plugin for it)
set -a;source .env.local;set +a 
go run BqToFtp.go
```

# Packaging

Building the doker file on Cloud Build and publish it on GCR with the specified tag. _Be careful of the project used_

Update the cloudbuild.yaml or the docker file for updating the build

```
gcloud builds submit --config cloudbuild.yaml
```

# Deploy

Deploy the container on Cloud Run with the correct env vars. _Be careful of the env var file used and the project of GCR_

```
# From the root of the project, else change the .env file location
gcloud beta run deploy bq-to-ftp --image gcr.io/$(gcloud config list --format='value(core.project)')/bq-to-ftp \
    --set-env-vars $(grep = .env | sed -z 's/\n/,/g') --region us-central1 --no-allow-unauthenticated
```
Or use the automatic Build and Deploy config. _Be careful, don't work for a first deployment because env var aren't set_
```
gcloud builds submit --config cloudbuildanddeploy.yaml
```

# Existing image
You can use the latest version of the container available here:
```
gcr.io/bq-to-ftp/bq-to-ftp
```

# Test

For generating the file, use this command. It's the same endpoint to set into Cloud Scheduler for performing the extract operation
```
# Local test
curl http://localhost:8080

# In production
curl -H "Authorization: Bearer $(gcloud config config-helper --format='value(credential.id_token)')" \
  $(gcloud beta run services describe bq-to-ftp --region us-central1 --format "value(status.address.hostname)")
```


# First deployment

A first build have to be done for each cloud run container. See package section.

For that, here apis to be activated
```
gcloud services enable --project $(gcloud config get-value project) \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  appengine.googleapis.com
```
For Berglas. Follow Berglas Github doc to initialize the bucket.
```
gcloud services enable --project $(gcloud config get-value project) \
  cloudkms.googleapis.com \
  storage-api.googleapis.com \
  storage-component.googleapis.com
```

For allowing cloud build to deploy on cloud run, role have to be granted

```
gcloud projects add-iam-policy-binding $(gcloud config get-value project) --role roles/run.admin \
    --member=serviceAccount:$(gcloud projects describe $(gcloud config get-value project) --format='value(projectNumber)')@cloudbuild.gserviceaccount.com
gcloud projects add-iam-policy-binding $(gcloud config get-value project) --role roles/iam.serviceAccountUser \
    --member=serviceAccount:$(gcloud projects describe $(gcloud config get-value project) --format='value(projectNumber)')@cloudbuild.gserviceaccount.com
```

## Service account
For cloud run
```
gcloud iam service-accounts create bqtoftp-cloudrun
gcloud projects add-iam-policy-binding $(gcloud config get-value project) --role roles/bigquery.dataViewer \
    --member=serviceAccount:bqtoftp-cloudrun@$(gcloud config get-value project).iam.gserviceaccount.com
gcloud projects add-iam-policy-binding $(gcloud config get-value project) --role roles/bigquery.jobUser \
    --member=serviceAccount:bqtoftp-cloudrun@$(gcloud config get-value project).iam.gserviceaccount.com
gcloud projects add-iam-policy-binding $(gcloud config get-value project) --role roles/storage.objectViewer \
    --member=serviceAccount:bqtoftp-cloudrun@$(gcloud config get-value project).iam.gserviceaccount.com
    
FOR BERGLAS SUPPORT
gcloud projects add-iam-policy-binding $(gcloud config get-value project) --role roles/cloudkms.cryptoKeyDecrypter \
    --member=serviceAccount:bqtoftp-cloudrun@$(gcloud config get-value project).iam.gserviceaccount.com
```

For cloud scheduler to call  Cloud Run
```
gcloud iam service-accounts create bqtoftp-scheduler-call
gcloud projects add-iam-policy-binding $(gcloud config get-value project) --role roles/run.invoker \
    --member=serviceAccount:bqtoftp-scheduler-call@$(gcloud config get-value project).iam.gserviceaccount.com
```

## CloudRun
```
SET the SQL file to bucket. Make sure that the bucket exists
gsutil cp query.sql $(grep QUERY_FILE_PATH .env | cut -d'=' -f2)


gcloud alpha run deploy bq-to-ftp --image gcr.io/bq-to-ftp/bq-to-ftp \
    --set-env-vars $(grep = .env | sed -z 's/\n/,/g') --region us-central1 --no-allow-unauthenticated \
    --service-account bqtoftp-cloudrun@$(gcloud config get-value project).iam.gserviceaccount.com
```

## Cloud scheduler
Scheduler, trigger according with the configuration in minute delta
```
gcloud beta scheduler jobs create http trigger-bq-to-ftp --schedule "every $(grep MINUTE_DELTA .env | cut -d'=' -f2) minutes" \
    --http-method=GET --uri $(gcloud beta run services describe bq-to-ftp --region us-central1 --format "value(status.address.hostname)") \
    --oidc-service-account-email=bqtoftp-scheduler-call@$(gcloud config get-value project).iam.gserviceaccount.com
```

# License

This library is licensed under Apache 2.0. Full license text is available in
[LICENSE](https://github.com/guillaumeblaquiere/bqtoftp/tree/master/LICENSE).