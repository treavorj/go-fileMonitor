package fileMonitor

import (
	"context"
	"os"
	"path/filepath"
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

	fileMonitor1, err := NewFileMonitor(context.Background(), logger, configFile.Name())
	if err != nil {
		t.Fatalf("failed to create fileMonitor1: %v", err)
	} else if fileMonitor1 == nil {
		t.Fatalf("fileMonitor1 is nil")
	}

	dir, err := fileMonitor1.NewDir(t.Name(), "testFolder", "publishLocation", time.Minute, &Processor{Type: ProcessorTypeCsv, Executor: csvConfig}, true, []MatchGroup{{Expression: "test"}})
	if err != nil {
		t.Fatalf("error creating dir in localHost: %v", err)
	}

	dir.Copiers = append(dir.Copiers, &CopierLocal{
		Destination: "testDest",
	})

	err = fileMonitor1.Update()
	if err != nil {
		t.Fatalf("error updating fileMonitor: %v", err)
	}

	fileMonitor2, err := NewFileMonitor(context.Background(), logger, configFile.Name())
	if err != nil {
		t.Fatalf("failed to create fileMonitor2: %v", err)
	} else if fileMonitor2 == nil {
		t.Fatalf("fileMonitor2 is nil")
	}

	if fileMonitor1.configPath != fileMonitor2.configPath {
		t.Errorf("configPath not equal\n1: %s\n2: %s", fileMonitor1.configPath, fileMonitor2.configPath)
	} else if fileMonitor1.MaxJobs != fileMonitor2.MaxJobs {
		t.Errorf("MaxJobs not equal\n1: %d\n2: %d", fileMonitor1.MaxJobs, fileMonitor2.MaxJobs)
	} else if fileMonitor1.NumWorkers != fileMonitor2.NumWorkers {
		t.Errorf("NumWorkers not equal\n1: %d\n2: %d", fileMonitor1.NumWorkers, fileMonitor2.NumWorkers)
	} else if len(fileMonitor1.Dirs) != len(fileMonitor2.Dirs) {
		t.Errorf("Len of Dirs not equal\n1: %d\n2: %d", len(fileMonitor1.Dirs), len(fileMonitor2.Dirs))
	} else if fileMonitor1.Dirs[t.Name()].Name != fileMonitor2.Dirs[t.Name()].Name {
		t.Errorf("Dir[0].Name not equal\n1: %s\n2: %s", fileMonitor1.Dirs[t.Name()].Name, fileMonitor2.Dirs[t.Name()].Name)
	}

	_, err = fileMonitor2.NewDir(t.Name()+"test2", "testFolder2", "publishLocation2", time.Minute, &Processor{}, false, []MatchGroup{})
	if err != nil {
		t.Errorf("error creating new Dir: %v", err)
	}

}

type testPublish struct {
	publishSuccessful bool
}

func (p *testPublish) Publish(dir *Dir, result [][]byte, id []string) error {
	p.publishSuccessful = true
	return nil
}

func TestProcessCsv(t *testing.T) {
	t.Parallel()

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano}).With().Caller().Logger()

	topic := "mc_test_" + uuid.New().String()
	parentFolder := t.TempDir()
	const folderName = "processCsv"
	folder := filepath.Join(parentFolder, folderName)
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
		t.Fatalf("failed to write to config file: %v", err)
	}
	err = configFile.Close()
	if err != nil {
		t.Fatalf("error closing file: %v", err)
	}

	fileMonitor, err := NewFileMonitor(context.Background(), logger, configFile.Name())
	if err != nil {
		t.Fatalf("failed to create fileMonitor: %v", err)
	} else if fileMonitor == nil {
		t.Fatalf("fileMonitor is nil")
	}

	matchGroups := []MatchGroup{
		{Expression: `.*test.*`},
		{Expression: `.*not_a_test.*`, Exclude: true},
	}

	dir, err := fileMonitor.NewDir(t.Name(), parentFolder, topic, time.Millisecond*100, &Processor{Type: ProcessorTypeCsv, Executor: csvConfig}, false, matchGroups)
	if err != nil {
		t.Fatalf("error creating new dir: %v", err)
	}

	testPublishVar := &testPublish{}
	fileMonitor.AddAllDirPublisher(testPublishVar)

	newTempFolder := t.TempDir()
	dir.Copiers = append(dir.Copiers, &CopierLocal{
		Destination: newTempFolder,
	})

	// load file
	const fileName = "testFile.csv"
	testFilepath := filepath.Join(folder, fileName)
	err = os.WriteFile(testFilepath, []byte(csvFile), os.ModeTemporary)
	if err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	fileNameExist := filepath.Join(folder, "ShouldBeThere.csv")
	err = os.WriteFile(fileNameExist, []byte(csvFile), os.ModeTemporary)
	if err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	time.Sleep(time.Millisecond * 600)
	_, err = os.Stat(testFilepath)
	if !os.IsNotExist(err) {
		t.Errorf("file should not exist but does")
	}

	testFile, err := os.Stat(filepath.Join(newTempFolder, folderName, fileName))
	if err != nil {
		t.Errorf("file should exist")
	}

	shouldBeThere, err := os.Stat(fileNameExist)
	if err != nil {
		t.Errorf("file should exist and no error should occur: %v", err)
	}

	if testFile.Size() != shouldBeThere.Size() {
		t.Errorf("testFile (%d) size does not equal shouldBeThere (%d) size", testFile.Size(), shouldBeThere.Size())
	}

	if !testPublishVar.publishSuccessful {
		t.Errorf("should have set publish to successful")
	}
}

func TestProcessFail(t *testing.T) {
	t.Parallel()

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano}).With().Caller().Logger()

	topic := "mc_test_" + uuid.New().String()
	parentFolder := t.TempDir()
	const folderName = "processCsv"
	folder := filepath.Join(parentFolder, folderName)
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
				Location: csvParse.Cell{Row: 0, Column: 9999},
				DataType: csvParse.DataTypeString,
				NameCell: csvParse.Cell{Row: 0, Column: 0},
			},
		},
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

	fileMonitor, err := NewFileMonitor(context.Background(), logger, configFile.Name())
	if err != nil {
		t.Fatalf("failed to create fileMonitor: %v", err)
	} else if fileMonitor == nil {
		t.Fatalf("fileMonitor is nil")
	}

	matchGroups := []MatchGroup{
		{Expression: `.*test.*`},
		{Expression: `.*not_a_test.*`, Exclude: true},
	}

	dir, err := fileMonitor.NewDir(t.Name(), parentFolder, topic, time.Millisecond*100, &Processor{Type: ProcessorTypeCsv, Executor: csvConfig}, false, matchGroups)
	if err != nil {
		t.Fatalf("error creating new dir: %v", err)
	}

	newTempFolder := t.TempDir()
	dir.ErrorCopiers = append(dir.Copiers, &CopierLocal{
		Destination: newTempFolder,
	})

	// load file
	const fileName = "testFile.csv"
	testFilepath := filepath.Join(folder, fileName)
	err = os.WriteFile(testFilepath, []byte(csvFile), os.ModeTemporary)
	if err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	fileNameExist := filepath.Join(folder, "ShouldBeThere.csv")
	err = os.WriteFile(fileNameExist, []byte(csvFile), os.ModeTemporary)
	if err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	time.Sleep(time.Millisecond * 600)
	_, err = os.Stat(testFilepath)
	if !os.IsNotExist(err) {
		t.Errorf("file should not exist but does")
	}

	_, err = os.Stat(filepath.Join(newTempFolder, folderName, fileName))
	if err != nil {
		t.Errorf("file should exist")
	}

	_, err = os.Stat(fileNameExist)
	if err != nil {
		t.Errorf("file should exist and no error should occur: %v", err)
	}
}
