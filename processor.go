package fileMonitor

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/treavorj/go-csvParse"
)

type Processor struct {
	Type     ProcessorType
	Executor ProcessorExecutor
}

func (p *Processor) UnmarshalJSON(data []byte) error {
	var aux struct {
		Type     ProcessorType
		Executor json.RawMessage
	}

	err := json.Unmarshal(data, &aux)
	if err != nil {
		return fmt.Errorf("error unmarshaling input data: %w", err)
	}

	p.Type = aux.Type
	p.Executor, err = p.Type.unmarshalType(aux.Executor)
	if err != nil {
		return fmt.Errorf("error unmarshaling Executor: %w", err)
	}

	return nil
}

type ProcessorExecutor interface {
	Process(file *os.File, filepath string) (result [][]byte, id []string, err error)
}

type ProcessorType int

const (
	ProcessorTypeNull ProcessorType = iota
	ProcessorTypeCsv
)

func (p ProcessorType) unmarshalType(data []byte) (ProcessorExecutor, error) {
	switch p {
	case ProcessorTypeNull:
		return nil, fmt.Errorf("no processor supplied")
	case ProcessorTypeCsv:
		processor := &csvParse.Csv{}
		return processor, json.Unmarshal(data, processor)
	default:
		return nil, fmt.Errorf("no valid processor for %v", p)
	}
}
