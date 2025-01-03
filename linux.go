//go:build linux

package fileMonitor

import (
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func setCreationTime(filePath string, creationTime time.Time) error {
	times := []unix.Timespec{
		{Sec: creationTime.Unix(), Nsec: int64(creationTime.Nanosecond())},
		{Sec: creationTime.Unix(), Nsec: int64(creationTime.Nanosecond())},
	}

	return unix.UtimesNano(filePath, times)
}

func getCreationTime(filePath string) (time.Time, error) {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return time.Time{}, err
	}

	stat := fileInfo.Sys().(*syscall.Stat_t)
	return time.Unix(stat.Mtim.Unix()), nil // Used Mtim due to Ctim not being reliable
}
