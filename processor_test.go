package fileMonitor

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/treavorj/go-csvParse"
	"github.com/treavorj/zerolog"
	"github.com/treavorj/zerolog/log"
	"github.com/treavorj/zerolog/pkgerrors"
)

func TestFileMonitorLoading(t *testing.T) {
	t.Parallel()

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano}).With().Caller().Logger()

	cells, err := csvParse.NewCellLocation(csvParse.Cell{Row: 1, Column: 1}, csvParse.DataTypeAuto, "testCell", csvParse.Cell{})
	if err != nil {
		t.Errorf("error creating new cell: %v", err)
	}
	concatCell, err := csvParse.NewConcatCellLocation([]csvParse.Cell{{Row: 2, Column: 2}, {Row: 3, Column: 3}}, ",", "testConcat", csvParse.Cell{})
	if err != nil {
		t.Errorf("error creating new concatCell: %v", err)
	}
	table, err := csvParse.NewTableLocation("testTable", csvParse.Cell{}, csvParse.Cell{Row: 4, Column: 4}, csvParse.Cell{Row: 5, Column: 5}, true, []string{}, true, []csvParse.DataType{}, true)
	if err != nil {
		t.Errorf("error creating new table: %v", err)
	}
	csvConfig := csvParse.NewCsvFile(
		[]csvParse.CellLocation{*cells},
		[]csvParse.ConcatCellLocation{*concatCell},
		[]csvParse.TableLocation{*table},
	)

	tempDir := t.TempDir()
	configFile, err := os.CreateTemp(tempDir, "*config.json")
	if err != nil {
		t.Fatalf("failed to create config file: %v", err)
	}
	defer configFile.Close()
	_, err = configFile.WriteString("{}")
	if err != nil {
		t.Fatalf("failed to write to config file: %v", err)
	}

	fileMonitor1, err := Init(context.Background(), logger, configFile.Name())
	if err != nil {
		t.Fatalf("failed to create fileMonitor1: %v", err)
	} else if fileMonitor1 == nil {
		t.Fatalf("fileMonitor1 is nil")
	}

	_, err = fileMonitor1.NewDir("testLocalDir", "testFolder", "publishLocation", time.Minute, &Processor{Type: ProcessorTypeCsv, Executor: csvConfig}, true, []MatchGroup{{Expression: "test"}})
	if err != nil {
		t.Errorf("error creating dir in localHost: %v", err)
	}

	fileMonitor2, err := Init(context.Background(), logger, configFile.Name())
	if err != nil {
		t.Logf("failed to create fileMonitor2: %v", err)
	} else if fileMonitor2 == nil {
		t.Fatalf("fileMonitor2 is nil")
	}

	if !reflect.DeepEqual(fileMonitor1, fileMonitor2) {
		t.Fatalf("fileMonitors are not equal")
	}
}

func TestProcessCsv(t *testing.T) {
	t.Parallel()

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano}).With().Caller().Logger()

	topic := "mc_test_" + uuid.New().String()
	parentFolder := t.TempDir()
	folder := filepath.Join(parentFolder, "processCsv")
	err := os.Mkdir(folder, os.ModePerm)
	if err != nil {
		t.Fatalf("failed to create folder: %v", err)
	}

	// setup csv file
	csvFile := `CellData,CellDataSuccessful
ConcatCell,ConcatColumn1,ConcatColumn2,ConcatColumn3
String,Int,Float
Test1,1,1.1
Test2,2,2.2
Test3,3,3.3
`
	currentTime := time.Now()
	csvFile += "timestamp," + currentTime.Format(time.RFC3339)

	// csv functions
	csvConfig := &csvParse.Csv{
		FilePathData: []csvParse.FilePathData{
			{
				Name:         "test",
				CaptureRegex: `.*/(?P<fileName>\w+).csv$`,
			},
		},
		CellLocations: []csvParse.CellLocation{
			{
				Location: csvParse.Cell{Row: 0, Column: 1},
				DataType: csvParse.DataTypeString,
				NameCell: csvParse.Cell{Row: 0, Column: 0},
			},
		},
		ConcatCellLocations: []csvParse.ConcatCellLocation{
			{
				Cells: []csvParse.Cell{
					{Row: 1, Column: 1},
					{Row: 1, Column: 2},
					{Row: 1, Column: 3},
				},
				Delimiter: ",",
				NameCell:  csvParse.Cell{Row: 1, Column: 0},
				DataType:  csvParse.DataTypeString,
			},
		},
		TableLocations: []csvParse.TableLocation{
			{
				Name:            "table",
				StartCell:       csvParse.Cell{Row: 2, Column: 0},
				EndCell:         csvParse.Cell{Row: 5, Column: -1},
				TableHasHeader:  true,
				ColumnDataTypes: []csvParse.DataType{csvParse.DataTypeString, csvParse.DataTypeInt64, csvParse.DataTypeFloat64},
				ParseAsArray:    true,
			},
		},
		TimeField: csvParse.TimeField{Layout: time.RFC3339, Cells: []csvParse.Cell{{Row: 6, Column: 1}}},
	}

	// setup file monitor
	configFile, err := os.CreateTemp("", "*config.json")
	if err != nil {
		t.Fatalf("failed to create config file: %v", err)
	}
	_, err = configFile.WriteString("{}")
	if err != nil {
		t.Fatalf("failed to write to config file")
	}

	fileMonitor, err := Init(context.Background(), logger, configFile.Name())
	if err != nil {
		t.Fatalf("failed to create fileMonitor: %v", err)
	} else if fileMonitor == nil {
		t.Fatalf("fileMonitor is nil")
	}

	matchGroups := []MatchGroup{
		{Expression: `.*test.*`},
		{Expression: `.*not_a_test.*`, Exclude: true},
	}

	_, err = fileMonitor.NewDir("testDir", parentFolder, topic, time.Millisecond*100, &Processor{Type: ProcessorTypeCsv, Executor: csvConfig}, false, matchGroups)
	if err != nil {
		t.Fatalf("error creating new dir: %v", err)
	}

	// load file
	fileName := filepath.Join(folder, "testFile.csv")
	err = os.WriteFile(fileName, []byte(csvFile), os.ModeTemporary)
	if err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	fileNameExist := filepath.Join(folder, "ShouldBeThere.csv")
	err = os.WriteFile(fileNameExist, []byte(csvFile), os.ModeTemporary)
	if err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	time.Sleep(time.Millisecond * 400)
	_, err = os.Stat(fileName)
	if !os.IsNotExist(err) {
		t.Errorf("file should not exist but does")
	}

	_, err = os.Stat(fileNameExist)
	if err != nil {
		t.Errorf("file should exist and no error should occur: %v", err)
	}
}
