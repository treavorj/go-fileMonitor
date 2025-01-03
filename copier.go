package fileMonitor

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Copier interface {
	Copy(inFile *os.File, dir, monitorDir string) error
	Type() CopierType
}

type CopierType uint

const (
	CopierTypeLocal CopierType = iota
)

type CopierLocal struct {
	Destination string
}

func (c *CopierLocal) Copy(inFile *os.File, dir, monitorDir string) error {
	var outDir string

	if string(monitorDir[0]) == "." {
		outDir = dir[len(monitorDir)-2:]
	} else {
		outDir = dir[len(monitorDir):]
	}

	outDir = filepath.Join(c.Destination, outDir)
	err := os.MkdirAll(outDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("unable to create directory: %w", err)
	}

	outFile, err := os.Create(filepath.Join(outDir, inFile.Name()))
	if err != nil {
		outFile.Close()
		return fmt.Errorf("error creating file: %w", err)
	}

	_, err = io.Copy(outFile, inFile)
	if err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	err = outFile.Close()
	if err != nil {
		return fmt.Errorf("failed to close outFile: %w", err)
	}

	creationTime, err := getCreationTime(inFile.Name())
	if err != nil {
		fileLog.Warn().Err(err).Str("inFilePath", inFilePath).Msg("unable to get file creation time")
	} else {
		err = setCreationTime(outFile.Name(), creationTime)
		if err != nil {
			fileLog.Warn().Err(err).Str("filepath", outFile.Name()).Msg("failed to set creation time")
		}
	}

}

// outFile, _, err := createLocalDir(fileName, dir, d.MonitorFolder, d.DestinationPath)
// if err != nil {
// 	fileLog.Warn().Err(err).Msg("error creating outFile")
// 	return err
// }
// fileLog = d.log.With().Str("outFileName", outFile.Name()).Logger()
// _, err = io.Copy(outFile, inFile)
// if err != nil {
// 	msg := "failed to copy file"
// 	fileLog.Error().Err(err).Msg(msg)
// 	return fmt.Errorf("%s:%w", msg, err)
// }

// err = outFile.Close()
// if err != nil {
// 	msg := "failed to close outFile"
// 	fileLog.Error().Err(err).Msg(msg)
// 	return fmt.Errorf("%s:%w", msg, err)
// }
// creationTime, err := getCreationTime(inFilePath)
// if err != nil {
// 	fileLog.Warn().Err(err).Str("inFilePath", inFilePath).Msg("unable to get file creation time")
// } else {
// 	err = setCreationTime(outFile.Name(), creationTime)
// 	if err != nil {
// 		fileLog.Warn().Err(err).Str("filepath", outFile.Name()).Msg("failed to set creation time")
// 	}
// }

// func createLocalDir(fileName, dir, monitorDir, destination string) (outFile *os.File, outDir string, err error) {
// 	if string(monitorDir[0]) == "." {
// 		outDir = dir[len(monitorDir)-2:]
// 	} else {
// 		outDir = dir[len(monitorDir):]
// 	}

// 	outDir = filepath.Join(destination, outDir)
// 	err = os.MkdirAll(outDir, os.ModePerm)
// 	if err != nil {
// 		return nil, "", fmt.Errorf("unable to create directory: %w", err)
// 	}

// 	outFile, err = os.Create(filepath.Join(outDir, fileName))
// 	if err != nil {
// 		outFile.Close()
// 		return nil, "", fmt.Errorf("error creating file: %w", err)
// 	}

// 	return outFile, outDir, nil
// }
