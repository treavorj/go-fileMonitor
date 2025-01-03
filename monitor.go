package fileMonitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/treavorj/zerolog"
)

var Monitors FileMonitor

const (
	defaultMaxJobs uint = 100
	defaultConfig       = "fileMonitor.json"
)

type FileMonitor struct {
	Dirs       map[string]*LocalDir
	MaxJobs    uint // maximum number of jobs that can be buffered without waiting
	NumWorkers uint // number of workers processing files

	workerWg    sync.WaitGroup
	workerTasks chan func() (err error)

	configLock sync.Mutex
	configPath string

	logger    zerolog.Logger
	ctxParent context.Context
	ctx       context.Context
	ctxCancel context.CancelFunc
}

func (f *FileMonitor) Connected() bool {
	if f.ctx == nil {
		return false
	}
	select {
	case <-f.ctx.Done():
		return false
	default:
		return true
	}
}

func (f *FileMonitor) startWorkers() {
	if f.ctx != nil {
		select {
		case <-f.ctx.Done():
			f.logger.Trace().Msg("restring workers")
		default:
			f.logger.Warn().Msg("workers already running")
			return
		}
	}
	f.ctx, f.ctxCancel = context.WithCancel(f.ctxParent)

	f.logger.Info().Uint("NumWorkers", f.NumWorkers).Uint("MaxJobs", f.MaxJobs).Msg("starting workers")

	if f.workerTasks == nil {
		f.workerTasks = make(chan func() (err error), f.MaxJobs)
	} else {
		if task, ok := <-f.workerTasks; !ok {
			f.workerTasks = make(chan func() (err error), f.MaxJobs)
		} else {
			f.workerTasks <- task
		}
	}

	f.workerWg.Add(int(f.NumWorkers))
	for i := uint(0); i < f.NumWorkers; i++ {
		go f.worker()
	}

	f.logger.Info().Msg("started all workers")

	go func() {
		f.workerWg.Wait()
		f.ctxCancel()
	}()
}

func (f *FileMonitor) worker() {
	defer f.workerWg.Done()

	for {
		select {
		case task, ok := <-f.workerTasks:
			if !ok {
				f.logger.Warn().Msg("worker closed due to closed channel. Should close with context")
			}
			err := task()
			if err != nil {
				f.logger.Warn().Err(err).Msg("error processing task")
				continue
			}
			f.logger.Trace().Msg("successfully published result")
		case <-f.ctx.Done():
			f.logger.Info().Msg("closing workers with context")
			return
		}
	}
}

func (f *FileMonitor) Update() error {
	if f.configPath == "" {
		return fmt.Errorf("file location cannot be empty")
	}

	data, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("unable to marshal data: %w", err)
	}

	f.configLock.Lock()
	defer f.configLock.Unlock()
	err = os.WriteFile(f.configPath, data, os.ModePerm)
	if err != nil {
		f.logger.Warn().Err(err).Msg("failed to update configuration")
		return err
	}
	f.logger.Info().Msg("updated config")
	return nil
}

func (f *FileMonitor) Start() error {
	if f.Dirs == nil {
		f.Dirs = make(map[string]*LocalDir)
	}
	f.startWorkers()

	f.logger.Info().Msg("Starting monitor of all dirs")

	for _, dir := range f.Dirs {
		dir.parent = f
		dir.Monitor()
	}

	f.logger.Info().Msg("successfully started monitoring all dirs")
	return nil
}

func Init(parentCtx context.Context, logger zerolog.Logger, configPath string) (*FileMonitor, error) {
	if configPath == "" {
		return nil, fmt.Errorf("configPath cannot be empty")
	}

	_, err := os.Stat(configPath)
	if os.IsNotExist(err) {
		logger.Warn().Str("app", "fileMonitor").Str("configPath", configPath).Msg("config file not found. Unable to load")
		return nil, nil
	}

	Monitors.configLock.Lock()
	defer Monitors.configLock.Unlock()
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("unable to open configuration file: %w", err)
	}
	defer file.Close()
	Monitors.configPath = configPath

	fileData, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("error reading data from file: %w", err)
	}

	err = json.Unmarshal(fileData, &Monitors)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal fileMonitor config file: %w", err)
	}

	Monitors.logger = logger.With().Str("app", "fileMonitor").DeDup().Logger()
	if Monitors.NumWorkers == 0 {
		Monitors.NumWorkers = uint(max(runtime.NumCPU()/2, 1))
	} else if Monitors.NumWorkers > uint(runtime.NumCPU()) {
		Monitors.logger.Warn().
			Uint("NumWorkersOld", Monitors.NumWorkers).
			Int("NumWorkers", runtime.NumCPU()).
			Msg("more monitors than cores so capping to core count")
		Monitors.NumWorkers = uint(runtime.NumCPU())
	}
	if Monitors.MaxJobs == 0 {
		Monitors.MaxJobs = defaultMaxJobs
	}
	Monitors.ctxParent = parentCtx

	Monitors.logger.Info().Msg("successfully initialized, starting up monitors")
	Monitors.Start()
	return &Monitors, nil
}

func (f *FileMonitor) AddDir(dir *LocalDir) error {
	dir.parent = f

	if f.Connected() {
		err := dir.Monitor()
		if err != nil {
			return err
		}
	}

	f.Dirs[dir.Name] = dir
	return f.Update()
}

func (f *FileMonitor) RemoveDir(dir *LocalDir) error {
	if dir.Active {
		dir.ctxCancel()
	}
	delete(f.Dirs, dir.Name)
	f.logger.Info().Str("dir", dir.Name).Msg("remove directory successfully")
	return f.Update()
}

func (f *FileMonitor) GetDirs() []string {
	var dirs []string
	for _, dir := range f.Dirs {
		dirs = append(dirs, dir.Name)
	}
	return dirs
}

func (f *FileMonitor) GetStats() *Stats {
	var stats Stats
	for _, dir := range f.Dirs {
		stats.Add(&dir.Stats)
	}
	return &stats
}

func (f *FileMonitor) NewDir(name, monitorFolder, publishLocation string, monitorFreq time.Duration, processor *Processor, overwriteExistingDir bool, matchGroups []MatchGroup) (*LocalDir, error) {
	if !overwriteExistingDir {
		existingDir, exists := f.Dirs[name]
		if exists {
			return existingDir, fmt.Errorf("dir with name already exists")
		}
	}

	dir := LocalDir{
		Name:             name,
		MonitorFolder:    monitorFolder,
		MonitorFrequency: monitorFreq,
		Active:           true,
		Processor:        processor,
		MatchGroups:      matchGroups,
		parent:           f,
	}

	f.Dirs[name] = &dir
	if f.Connected() {
		err := dir.Monitor()
		if err != nil {
			return &dir, fmt.Errorf("error monitoring dir: %w", err)
		}
	}
	return &dir, f.Update()
}
