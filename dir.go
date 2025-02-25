package fileMonitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/treavorj/zerolog"
)

// Regex matching for file to be processed
//
// Uses dir + file name with the separator guaranteed to be "/"
type MatchGroup struct {
	Expression string
	Exclude    bool // If true value must not exist. If false value must exist

	compiled *regexp.Regexp
}

func (m *MatchGroup) Match(filePath string) (match bool, err error) {
	if m.compiled == nil {
		m.compiled, err = regexp.Compile(m.Expression)
		if err != nil {
			return false, fmt.Errorf("failed to compile regex: %s, error: %v", m.Expression, err)
		}
	}

	name := strings.ReplaceAll(filePath, string(filepath.Separator), "/")
	return m.compiled.MatchString(name) != m.Exclude, nil
}

type Dir struct {
	Name             string
	MonitorFolder    string
	MonitorFrequency time.Duration
	KeepEmptyDirs    bool
	Active           bool
	MatchGroups      []MatchGroup

	Processor  *Processor
	Publishers []Publisher `json:"-"`
	Copiers    []Copier

	// Copier to use if an error occurs after a match as original file will be delete
	ErrorCopiers []Copier

	Stats Stats

	log       zerolog.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc

	parent *FileMonitor
}

func (d *Dir) UnmarshalJSON(input []byte) error {
	type Alias Dir
	aux := &struct {
		*Alias

		Copiers      []CopierAlias
		ErrorCopiers []CopierAlias
	}{
		Alias: (*Alias)(d),
	}

	var err error
	if err = json.Unmarshal(input, &aux); err != nil {
		return err
	}

	copiers := make([]Copier, len(aux.Copiers))
	for n := range aux.Copiers {
		copiers[n], err = aux.Copiers[n].GetCopier()
		if err != nil {
			return fmt.Errorf("unable to get type for copier: %w", err)
		}
	}
	d.Copiers = copiers

	errCopiers := make([]Copier, len(aux.ErrorCopiers))
	for n := range aux.ErrorCopiers {
		errCopiers[n], err = aux.ErrorCopiers[n].GetCopier()
		if err != nil {
			return fmt.Errorf("unable to get type for error copier: %w", err)
		}
	}
	d.ErrorCopiers = errCopiers

	return nil
}

func (d *Dir) Monitor() error {
	d.log = d.parent.logger.With().Str("monitorFolder", d.MonitorFolder).DeDup().Logger()
	d.Stats = Stats{}
	d.log.Info().Msg("starting monitor")
	d.ctx, d.ctxCancel = context.WithCancel(d.parent.ctx)

	go d.monitor()
	return nil
}

func (d *Dir) monitor() {
	d.Active = true
	defer func() {
		d.Active = false
	}()

	ticker := time.NewTicker(d.MonitorFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			startRead := time.Now()
			d.log.Trace().Time("startRead", startRead).Msg("checking for file changes")
			err := d.readDir(d.MonitorFolder, true)
			if err != nil {
				d.log.Error().Err(err).Dur("processTime", time.Since(startRead)).Msg("failed to read directory")
				continue
			}
			d.log.Trace().Dur("processTime", time.Since(startRead)).Msg("finished reading directory")
		case <-d.ctx.Done():
			d.log.Info().Msg("stopping monitor")
			return
		}

	}
}

func (d *Dir) readDir(dir string, root bool) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		d.log.Error().Err(err).Str("dir", dir).Msg("failed to read directory")
		return err
	}
	if len(files) == 0 {
		d.log.Trace().Str("dir", dir).Msg("no files found")
		if root || !d.KeepEmptyDirs {
			return nil
		}
		err := os.Remove(dir)
		if err != nil {
			d.log.Error().Err(err).Str("dir", dir).Msg("failed to delete empty directory")
			return err
		}
		d.log.Trace().Str("dir", dir).Msg("deleted empty directory")
		return nil
	}
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			_ = d.readDir(filepath.Join(dir, fileInfo.Name()), false)
		} else {
			d.parent.workerTasks <- func(worker uint) {
				d.processFiles(worker, fileInfo, dir)
			}
		}
	}
	return nil
}

func (d *Dir) processFiles(worker uint, fileInfo fs.DirEntry, dir string) {
	startTime := time.Now()
	fileLog := d.log.With().Uint("worker", worker).Str("filename", fileInfo.Name()).Str("dir", dir).Logger()
	inFilePath := filepath.Join(dir, fileInfo.Name())

	for _, matchGroup := range d.MatchGroups {
		match, err := matchGroup.Match(inFilePath)
		if err != nil {
			fileLog.Warn().Err(err).Msg("error matching file")
			return
		}
		if !match {
			return
		}
	}

	fileStats, err := os.Stat(inFilePath)
	if err != nil {
		fileLog.Error().Err(err).Msg("Error getting file stats")

		err = d.processError(inFilePath, d.MonitorFolder)
		if err != nil {
			fileLog.Error().Err(err).Msg("error occurred while processing the error copier")
		}
		return
	}
	d.Stats.Inc(uint64(fileStats.Size()))
	fileLog.Trace().
		Int64("size", fileStats.Size()).
		Str("name", fileStats.Name()).
		Str("mode", fileStats.Mode().String()).
		Time("lastWriteTime", fileStats.ModTime()).
		Msg("fileStats")

	defer func() {
		err = os.Remove(inFilePath)
		if err != nil {
			fileLog.Error().Err(err).TimeDiff("processingTime", time.Now(), startTime).Msg("failed to delete file")

			err = d.processError(inFilePath, d.MonitorFolder)
			if err != nil {
				fileLog.Error().Err(err).TimeDiff("processingTime", time.Now(), startTime).Msg("error while processing the error copier")
			}
		}
	}()

	if d.Processor != nil {
		fileLog.Trace().Msg("processing file")
		results, id, err := d.Processor.Executor.Process(inFilePath)
		if err != nil {
			fileLog.Warn().Err(err).TimeDiff("processingTime", time.Now(), startTime).Msg("error when processing the file")

			err := d.processError(inFilePath, d.MonitorFolder)
			if err != nil {
				fileLog.Error().Err(err).TimeDiff("processingTime", time.Now(), startTime).Msg("error while processing the error copier")
			}

			return
		}
		fileLog.Trace().Dur("processingTime", time.Since(startTime)).Msg("successfully processed file. Publishing results")

		for _, publisher := range d.Publishers {
			err = publisher.Publish(d, results, id)
			if err != nil {
				fileLog.Warn().Err(err).TimeDiff("processingTime", time.Now(), startTime).Msg("error while publishing the results")
				return
			}

			err := d.processError(inFilePath, d.MonitorFolder)
			if err != nil {
				fileLog.Error().Err(err).TimeDiff("processingTime", time.Now(), startTime).Msg("error while processing the error copier")
			}
		}
		fileLog.Trace().Dur("processingTime", time.Since(startTime)).Msg("successfully published results")
	}

	for _, copier := range d.Copiers {
		err = copier.Copy(inFilePath, d.MonitorFolder)
		if err != nil {
			fileLog.Error().Err(err).TimeDiff("processingTime", time.Now(), startTime).Msg("error copying the file")

			err := d.processError(inFilePath, d.MonitorFolder)
			if err != nil {
				fileLog.Error().Err(err).TimeDiff("processingTime", time.Now(), startTime).Msg("error while processing the error copier")
			}

			return
		}
	}

	fileLog.Trace().Dur("processingTime", time.Since(startTime)).Msg("successfully processed entire file")
}

func (d *Dir) processError(inFilePath, monitorFolder string) error {
	var errs []error

	for _, copier := range d.ErrorCopiers {
		err := copier.Copy(inFilePath, monitorFolder)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	} else if len(errs) == 1 {
		return fmt.Errorf("error occurred while processing errors: %w", errs[0])
	}

	errMsg := ""
	for _, err := range errs {
		errMsg += fmt.Sprintf("\n%v", err)
	}

	return fmt.Errorf("%d errors occurred while processing files: %s", len(errs), errMsg)
}
