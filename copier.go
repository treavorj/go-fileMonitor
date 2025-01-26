package fileMonitor

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/jlaffaye/ftp"
)

type CopierType int

const (
	CopierTypeLocal = iota
	CopierTypeFTP
)

type Copier interface {
	Copy(inFile *os.File, dir, monitorDir string) error
	Type() CopierType
}

type CopierAlias struct {
	Type    CopierType
	Details json.RawMessage
}

func (c *CopierAlias) GetCopier() (Copier, error) {
	switch c.Type {
	case CopierTypeLocal:
		copier := &LocalCopier{}
		err := json.Unmarshal(c.Details, copier)
		return copier, err
	case CopierTypeFTP:
		copier := &FtpCopier{}
		err := json.Unmarshal(c.Details, copier)
		return copier, err
	default:
		return nil, fmt.Errorf("invalid type: %d", c.Type)
	}
}

func getOutFileName(dir, monitorDir, destination, inFileName string) (string, error) {
	var outDir string

	if string(monitorDir[0]) == "." {
		outDir = dir[len(monitorDir)-2:]

	} else {
		outDir = dir[len(monitorDir):]
	}

	outDir = filepath.Join(destination, outDir)
	err := os.MkdirAll(outDir, os.ModePerm)
	if err != nil {
		return "", fmt.Errorf("unable to create directory: %w", err)
	}

	segments := strings.Split(inFileName, string(os.PathSeparator))
	return filepath.Join(outDir, segments[len(segments)-1]), nil
}

type LocalCopier struct {
	Destination string
}

func (c *LocalCopier) Copy(inFile *os.File, dir, monitorDir string) error {
	outFileName, err := getOutFileName(dir, monitorDir, c.Destination, inFile.Name())
	if err != nil {
		return fmt.Errorf("error getting the output file name: %w", err)
	}

	outFile, err := os.Create(outFileName)
	if err != nil {
		outFile.Close()
		return fmt.Errorf("error creating file: %w", err)
	}

	_, err = inFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("error seeking file: %w", err)
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

func (c *LocalCopier) Type() CopierType {
	return CopierTypeLocal
}

type FtpCopier struct {
	Server      string
	Username    string
	Password    string
	Destination string
}

func (f *FtpCopier) Copy(inFile *os.File, dir, monitorDir string) error {
	outFileName, err := getOutFileName(dir, monitorDir, f.Destination, inFile.Name())
	if err != nil {
		return fmt.Errorf("error getting the output file name: %w", err)
	}
	conn, err := ftp.Dial(f.Server)
	if err != nil {
		return fmt.Errorf("failed to connect to FTP server: %w", err)
	}
	defer conn.Quit()

	err = conn.Login(f.Username, f.Password)
	if err != nil {
		return fmt.Errorf("failed to log in to FTP server: %w", err)
	}

	_, err = inFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("error seeking file: %w", err)
	}

	err = conn.Stor(outFileName, inFile)
	if err != nil {
		return fmt.Errorf("failed to upload file to FTP server: %w", err)
	}

	return nil
}

func (f *FtpCopier) Type() CopierType {
	return CopierTypeFTP
}
