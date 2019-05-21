package services

import "testing"

func Test_formatFtpPath(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name              string
		args              args
		wantFormattedPath string
	}{
		{
			name: "with already /",
			args: args{
				path: "/path/",
			},
			wantFormattedPath: "/path/",
		},
		{
			name: "with / at the beginning",
			args: args{
				path: "/path",
			},
			wantFormattedPath: "/path/",
		},
		{
			name: "with / at the end",
			args: args{
				path: "path/",
			},
			wantFormattedPath: "/path/",
		},
		{
			name: "without /",
			args: args{
				path: "path",
			},
			wantFormattedPath: "/path/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotFormattedPath := formatFtpPath(tt.args.path); gotFormattedPath != tt.wantFormattedPath {
				t.Errorf("formatFtpPath() = %v, want %v", gotFormattedPath, tt.wantFormattedPath)
			}
		})
	}
}
