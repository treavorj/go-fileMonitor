package fileMonitor

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jlaffaye/ftp"
)

type CopierType int

const (
	CopierTypeNull = iota
	CopierTypeLocal
	CopierTypeFTP
)

type Copier interface {
	Copy(dir, monitorDir string) error
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

func getOutFileName(inFilePath, monitorDir, destination string) string {
	if monitorDir[0:1] == "." {
		return filepath.Join(destination, inFilePath[len(monitorDir)-2:])
	}

	return filepath.Join(destination, inFilePath[len(monitorDir):])
}

type CopierLocal struct {
	Destination string
}

func (c *CopierLocal) Copy(filePath, monitorDir string) error {
	outFileName := getOutFileName(filePath, monitorDir, c.Destination)

	err := os.MkdirAll(filepath.Dir(outFileName), os.ModePerm)
	if err != nil {
		return fmt.Errorf("unable to create directory: %w", err)
	}

	outFile, err := os.Create(outFileName)
	if err != nil {
		outFile.Close()
		return fmt.Errorf("error creating file: %w", err)
	}

	inFile, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}

	_, err = io.Copy(outFile, inFile)
	if err != nil {
		inFile.Close()
		return fmt.Errorf("failed to copy file: %w", err)
	}

	err = inFile.Close()
	if err != nil {
		return fmt.Errorf("error closing inFile: %w", err)
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

func (c *CopierFtp) Copy(inFilePath, monitorDir string) error {
	if c.EncryptionFunc == nil || c.DecryptionFunc == nil {
		return fmt.Errorf("must supply both an encryption and decryption function")
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

	inFile, err := os.Open(inFilePath)
	if err != nil {
		return fmt.Errorf("failed to open the file: %w", err)
	}
	defer inFile.Close()

	outFileName := getOutFileName(inFilePath, monitorDir, c.Destination)
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
