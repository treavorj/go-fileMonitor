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

	// Ignore errors as they are not critical
	creationTime, err := getCreationTime(inFile.Name())
	if err == nil {
		setCreationTime(outFile.Name(), creationTime)
	}

	return nil
}
