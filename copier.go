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
	CopierTypeNull = iota
	CopierTypeLocal
	CopierTypeFTP
)

type Copier interface {
	Copy(inFile *os.File, dir, monitorDir string) error
	GetType() CopierType
	MarshalJSON() ([]byte, error) // Must inject type into object
}

type CopierAlias struct {
	Type    CopierType
	Details json.RawMessage
}

func (c *CopierAlias) UnmarshalJSON(data []byte) error {
	// Create a temporary map to hold the JSON key-value pairs
	var tempMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &tempMap); err != nil {
		return err
	}

	// Extract the Type field
	typeField, ok := tempMap["Type"]
	if !ok {
		return fmt.Errorf("missing Type field in Copier")
	}
	if err := json.Unmarshal(typeField, &c.Type); err != nil {
		return fmt.Errorf("invalid Type field: %w", err)
	}

	// Remove the Type field from the map to get the Details
	delete(tempMap, "Type")

	// Marshal the remaining fields back into JSON and assign to Details
	detailsData, err := json.Marshal(tempMap)
	if err != nil {
		return fmt.Errorf("failed to marshal Details: %w", err)
	}
	c.Details = json.RawMessage(detailsData)

	return nil
}

func (c *CopierAlias) GetCopier() (Copier, error) {
	switch c.Type {
	case CopierTypeNull:
		return nil, fmt.Errorf("no type provided")
	case CopierTypeLocal:
		copier := &CopierLocal{}
		err := json.Unmarshal(c.Details, copier)
		return copier, err
	case CopierTypeFTP:
		copier := &CopierFtp{}
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

type CopierLocal struct {
	Destination string
}

func (c *CopierLocal) Copy(inFile *os.File, dir, monitorDir string) error {
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

func (c *CopierLocal) GetType() CopierType {
	return CopierTypeLocal
}

func (c *CopierLocal) MarshalJSON() ([]byte, error) {
	type Alias CopierLocal
	return json.Marshal(&struct {
		Type CopierType `json:"Type"`
		*Alias
	}{
		Type:  c.GetType(),
		Alias: (*Alias)(c),
	})
}

type CopierFtp struct {
	Server      string
	Username    string
	Password    string // Does not store the password directly and only stores encrypted using the encryptionFunc
	Destination string

	EncryptionFunc func(string) (string, error) `json:"-"` // Not stored in JSON
	DecryptionFunc func(string) (string, error) `json:"-"` // Not stored in JSON
}

func (c *CopierFtp) Copy(inFile *os.File, dir, monitorDir string) error {
	if c.EncryptionFunc == nil || c.DecryptionFunc == nil {
		return fmt.Errorf("must supply both an encryption and decryption function")
	}

	outFileName, err := getOutFileName(dir, monitorDir, c.Destination, inFile.Name())
	if err != nil {
		return fmt.Errorf("error getting the output file name: %w", err)
	}
	conn, err := ftp.Dial(c.Server)
	if err != nil {
		return fmt.Errorf("failed to connect to FTP server: %w", err)
	}
	defer conn.Quit()

	err = conn.Login(c.Username, c.Password)
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

func (c *CopierFtp) GetType() CopierType {
	return CopierTypeFTP
}

func (f *CopierFtp) MarshalJSON() ([]byte, error) {
	type Alias CopierFtp
	return json.Marshal(&struct {
		Type CopierType `json:"Type"`
		*Alias
	}{
		Type:  f.GetType(),
		Alias: (*Alias)(f),
	})
}
