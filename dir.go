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

type LocalDir struct {
	Name             string
	MonitorFolder    string
	MonitorFrequency time.Duration
	KeepEmptyDirs    bool
	Active           bool
	MatchGroups      []MatchGroup

	Processor  *Processor
	Publishers []Publisher
	Copiers    []Copier
	Stats      Stats

	log       zerolog.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc

	parent *FileMonitor
}

func (d *LocalDir) Monitor() error {
	d.log = d.parent.logger.With().Str("monitorFolder", d.MonitorFolder).DeDup().Logger()
	d.Stats = Stats{}
	d.log.Info().Msg("starting monitor")
	d.ctx, d.ctxCancel = context.WithCancel(d.parent.ctx)

	go d.monitor()
	return nil
}

func (d *LocalDir) monitor() {
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

func (d *LocalDir) readDir(dir string, root bool) error {
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
			d.parent.workerTasks <- func() (err error) {
				return d.processFiles(fileInfo, dir)
			}
		}
	}
	return nil
}

func (d *LocalDir) processFiles(fileInfo fs.DirEntry, dir string) (err error) {
	startTime := time.Now()
	fileName := fileInfo.Name()
	fileLog := d.log.With().Str("filename", fileName).Str("dir", dir).Logger()

	for _, matchGroup := range d.MatchGroups {
		match, err := matchGroup.Match(dir, fileName)
		if err != nil {
			fileLog.Warn().Err(err).Msg("error matching file")
			return fmt.Errorf("error matching file: %w", err)
		}
		if !match {
			return nil
		}
	}

	inFilePath := filepath.Join(dir, fileName)
	inFile, err := os.Open(inFilePath)
	if err != nil {
		msg := "failed to open file"
		fileLog.Error().Err(err).Msg(msg)
		return fmt.Errorf("%s:%w", msg, err)
	}
	defer func() {
		if err := inFile.Close(); err != nil {
			fileLog.Error().Err(err).Msg("error closing in file")
		}
	}()

	fileStats, err := inFile.Stat()
	if err != nil {
		msg := "Error getting file stats"
		fileLog.Error().Err(err).Msg(msg)
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
		fileLog.Trace().Str("filename", inFile.Name()).Msg("processing file")
		results, id, err := d.Processor.Executor.Process(inFile, dir)
		if err != nil {
			msg := "error when processing file"
			fileLog.Error().Err(err).TimeDiff("processingTime", time.Now(), startTime).Msg(msg)
			return fmt.Errorf("%s:%w", msg, err)
		}
		fileLog.Trace().Dur("processingTime", time.Since(startTime)).Msg("successfully processed file. Publishing results")

		for _, publisher := range d.Publishers {
			err = publisher.Publish(results, id)
			if err != nil {
				return fmt.Errorf("error publishing result: %w", err)
			}
		}
		fileLog.Trace().Dur("processingTime", time.Since(startTime)).Msg("successfully published results")
	}

	for _, copier := range d.Copiers {
		err = copier.Copy(inFile)
		if err != nil {
			return fmt.Errorf("error copying file: %w", err)
		}
	}

	err = os.Remove(filepath.Join(dir, fileName))
	if err != nil {
		msg := "failed to delete file"
		fileLog.Error().Err(err).Msg(msg)
		return fmt.Errorf("%s:%w", msg, err)
	}

	fileLog.Trace().Dur("processingTime", time.Since(startTime)).Msg("successfully processed entire file")
	return nil
}
