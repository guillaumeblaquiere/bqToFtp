package controllers

import (
	"bqToFtp/helpers"
	"bqToFtp/mocks"
	"bqToFtp/services"
	"errors"
	"github.com/stretchr/testify/mock"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

func Test_createFileInMemory(t *testing.T) {
	type args struct {
		header      bool
		separator   []byte
		rowIterator services.IRowIterator
	}
	tests := []struct {
		name             string
		args             args
		wantFileInMemory []byte
		wantErr          bool
	}{
		{
			name: "Content parsed with header",
			args: args{
				header:      true,
				separator:   []byte(","),
				rowIterator: createBqRow(),
			},
			wantFileInMemory: []byte(
				"id,Name,Value\n" +
					"0,name0,0\n" +
					"1,name1,1\n" +
					"2,name2,2\n"),
			wantErr: false,
		},
		{
			name: "Content parsed without header and with semicolon",
			args: args{
				header:      false,
				separator:   []byte(";"),
				rowIterator: createBqRow(),
			},
			wantFileInMemory: []byte(
				"0;name0;0\n" +
					"1;name1;1\n" +
					"2;name2;2\n"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFileInMemory, err := createFileInMemory(tt.args.header, tt.args.separator, tt.args.rowIterator)
			if (err != nil) != tt.wantErr {
				t.Errorf("createFileInMemory() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotFileInMemory, tt.wantFileInMemory) {
				t.Errorf("createFileInMemory() = %v, want %v", string(gotFileInMemory), string(tt.wantFileInMemory))
			}
		})
	}
}

func createBqRow() services.IRowIterator {
	dummy := &DummyRowIterator{
		Row: [][]bigquery.Value{
			{
				"0",
				"name0",
				"0",
			},
			{
				"1",
				"name1",
				"1",
			},
			{
				"2",
				"name2",
				"2",
			},
		},
	}
	return dummy
}

type DummyRowIterator struct {
	Row [][]bigquery.Value
}

func (dummy *DummyRowIterator) Next(dst interface{}) error {
	if len(dummy.Row) > 0 {
		mappedResult := dst.(*[]bigquery.Value)
		row := dummy.Row[0]
		dummy.Row = dummy.Row[1:]
		*mappedResult = append((*mappedResult)[:0], row...)
		return nil
	}
	return iterator.Done
}

func (dummy *DummyRowIterator) PageInfo() *iterator.PageInfo {
	panic("PageInfo not implemented")
}

func (dummy *DummyRowIterator) GetSchema() bigquery.Schema {
	return bigquery.Schema{
		&bigquery.FieldSchema{
			Name: "id",
			Type: bigquery.StringFieldType,
		},
		&bigquery.FieldSchema{
			Name: "Name",
			Type: bigquery.StringFieldType,
		},
		&bigquery.FieldSchema{
			Name: "Value",
			Type: bigquery.StringFieldType,
		},
	}
}

func Test_bqToFtpController_sendFile(t *testing.T) {
	mockFtp := &mocks.IFTPService{}
	mockFtp.On("Send", "correct", mock.Anything).Return(nil)
	mockFtp.On("Send", "error", mock.Anything).Return(errors.New("error"))

	type fields struct {
		IBqToFtpController IBqToFtpController
		configService      helpers.IConfigService
		bigQueryService    services.IBigQueryService
		ftpService         services.IFTPService
		storageService     services.IStorageService
		withHeader         bool
		separator          []byte
		filePrefix         string
		timeFormat         string
	}
	type args struct {
		fileName     string
		fileInMemory []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Correct Send",
			fields: fields{
				ftpService: mockFtp, //mock
			},
			args: args{
				fileName:     "correct",
				fileInMemory: []byte("correct"),
			},
			wantErr: false,
		},
		{
			name: "Error Send",
			fields: fields{
				ftpService: mockFtp, //mock
			},
			args: args{
				fileName:     "error",
				fileInMemory: []byte("error"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := &bqToFtpController{
				IBqToFtpController: tt.fields.IBqToFtpController,
				configService:      tt.fields.configService,
				bigQueryService:    tt.fields.bigQueryService,
				ftpService:         tt.fields.ftpService,
				storageService:     tt.fields.storageService,
				withHeader:         tt.fields.withHeader,
				separator:          tt.fields.separator,
				filePrefix:         tt.fields.filePrefix,
				timeFormat:         tt.fields.timeFormat,
			}
			if err := controller.sendFile(tt.args.fileName, tt.args.fileInMemory); (err != nil) != tt.wantErr {
				t.Errorf("bqToFtpController.sendFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
