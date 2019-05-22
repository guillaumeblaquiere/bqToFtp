package controllers

import (
	"bqToFtp/services"
	"google.golang.org/api/iterator"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
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
			name:"Content parsed with header",
			args:args{
				header:true,
				separator:[]byte(","),
				rowIterator:createBqRow(),
			},
			wantFileInMemory:[]byte(
				"id,Name,Value\n" +
				"0,name0,0\n"+
				"1,name1,1\n"+
				"2,name2,2\n"),
			wantErr:false,
		},
		{
			name:"Content parsed without header and with semicolon",
			args:args{
				header:false,
				separator:[]byte(";"),
				rowIterator:createBqRow(),
			},
			wantFileInMemory:[]byte(
				"0;name0;0\n"+
				"1;name1;1\n"+
				"2;name2;2\n"),
			wantErr:false,
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
	if len(dummy.Row)>0{
		mappedResult := dst.(*[]bigquery.Value)
		row := dummy.Row[0]
		*mappedResult = make([]bigquery.Value,len(row))
		copy((*mappedResult)[:],row)
		dummy.Row = dummy.Row[1:]
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
			Name:"id",
			Type:bigquery.StringFieldType,
		},
		&bigquery.FieldSchema{
			Name:"Name",
			Type:bigquery.StringFieldType,
		},
		&bigquery.FieldSchema{
			Name:"Value",
			Type:bigquery.StringFieldType,
		},
	}
}
