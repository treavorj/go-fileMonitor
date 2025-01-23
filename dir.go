package fileMonitor

import (
	"context"
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

func (m *MatchGroup) Match(dir, fileName string) (match bool, err error) {
	if m.compiled == nil {
		m.compiled, err = regexp.Compile(m.Expression)
		if err != nil {
			return false, fmt.Errorf("failed to compile regex: %s, error: %v", m.Expression, err)
		}
	}

	name := strings.ReplaceAll(filepath.Join(dir, fileName), string(filepath.Separator), "/")
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
	Publishers []Publisher
	Copiers    []Copier

	// Copier to use if an error occurs after a match as original file will be delete
	ErrorCopiers []Copier

	Stats Stats

	log       zerolog.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc

	parent *FileMonitor
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

func (d *Dir) processFiles(worker uint, fileInfo fs.DirEntry, dir string) (err error) {
	startTime := time.Now()
	fileLog := d.log.With().Uint("worker", worker).Str("filename", fileInfo.Name()).Str("dir", dir).Logger()

	for _, matchGroup := range d.MatchGroups {
		match, err := matchGroup.Match(dir, fileInfo.Name())
		if err != nil {
			fileLog.Warn().Err(err).Msg("error matching file")
			return fmt.Errorf("error matching file: %w", err)
		}
		if !match {
			return nil
		}
	}

	inFilePath := filepath.Join(dir, fileInfo.Name())
	inFile, err := os.Open(inFilePath)
	if err != nil {
		msg := "failed to open file"
		fileLog.Error().Err(err).Msg(msg)
		return fmt.Errorf("%s:%w", msg, err)
	}

	defer func() {
		err = os.Remove(filepath.Join(dir, fileInfo.Name()))
		if err != nil {
			msg := "failed to delete file"
			err1 := d.processError(inFile, dir, d.MonitorFolder)
			if err1 != nil {
				fileLog.Error().Err(err).Str("processError", err1.Error()).TimeDiff("processingTime", time.Now(), startTime).Msg(msg)
			} else {
				fileLog.Error().Err(err).TimeDiff("processingTime", time.Now(), startTime).Msg(msg)
			}
		}
	}()

	fileStats, err := inFile.Stat()
	if err != nil {
		msg := "Error getting file stats"
		err1 := d.processError(inFile, dir, d.MonitorFolder)
		if err1 != nil {
			fileLog.Error().Err(err).Str("processError", err1.Error()).Msg(msg)
		} else {
			fileLog.Error().Err(err).Msg(msg)
		}
		if err := inFile.Close(); err != nil {
			fileLog.Error().Err(err).Msg("error closing inFile")
		}
		return fmt.Errorf("%s:%w", msg, err)
	}
	d.Stats.Inc(uint64(fileStats.Size()))
	fileLog.Trace().
		Int64("size", fileStats.Size()).
		Str("name", fileStats.Name()).
		Str("mode", fileStats.Mode().String()).
		Time("lastWriteTime", fileStats.ModTime()).
		Msg("fileStats")

	if d.Processor != nil {
		fileLog.Trace().Msg("processing file")
		results, id, err := d.Processor.Executor.Process(inFile, dir)
		if err != nil {
			msg := "error when processing file"
			err1 := d.processError(inFile, dir, d.MonitorFolder)
			if err1 != nil {
				fileLog.Error().Err(err).Str("processError", err1.Error()).TimeDiff("processingTime", time.Now(), startTime).Msg(msg)
			} else {
				fileLog.Error().Err(err).TimeDiff("processingTime", time.Now(), startTime).Msg(msg)
			}
			if err := inFile.Close(); err != nil {
				fileLog.Error().Err(err).Msg("error closing inFile")
			}
			return fmt.Errorf("%s:%w", msg, err)
		}
		fileLog.Trace().Dur("processingTime", time.Since(startTime)).Msg("successfully processed file. Publishing results")

		for _, publisher := range d.Publishers {
			err = publisher.Publish(results, id)
			if err != nil {
				if err := inFile.Close(); err != nil {
					fileLog.Error().Err(err).Msg("error closing inFile")
				}
				return fmt.Errorf("error publishing result: %w", err)
			}
		}
		fileLog.Trace().Dur("processingTime", time.Since(startTime)).Msg("successfully published results")
	}

	for _, copier := range d.Copiers {
		err = copier.Copy(inFile, dir, d.MonitorFolder)
		if err != nil {
			msg := "error copying file"
			err1 := d.processError(inFile, dir, d.MonitorFolder)
			if err1 != nil {
				fileLog.Error().Err(err).Str("processError", err1.Error()).TimeDiff("processingTime", time.Now(), startTime).Msg(msg)
			} else {
				fileLog.Error().Err(err).TimeDiff("processingTime", time.Now(), startTime).Msg(msg)
			}
			if err := inFile.Close(); err != nil {
				fileLog.Error().Err(err).Msg("error closing inFile")
			}
			return fmt.Errorf("%s: %w", msg, err)
		}
	}
	if err := inFile.Close(); err != nil {
		fileLog.Error().Err(err).Msg("error closing inFile")
	}

	fileLog.Trace().Dur("processingTime", time.Since(startTime)).Msg("successfully processed entire file")
	return nil
}

func (d *Dir) processError(inFile *os.File, dir, monitorFolder string) error {
	var errs []error

	for _, copier := range d.ErrorCopiers {
		err := copier.Copy(inFile, dir, monitorFolder)
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
