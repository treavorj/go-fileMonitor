//go:build windows

package fileMonitor

import (
	"time"

	"golang.org/x/sys/windows"
)

func setCreationTime(filePath string, creationTime time.Time) error {
	h, err := windows.CreateFile(windows.StringToUTF16Ptr(filePath), windows.GENERIC_WRITE, windows.FILE_SHARE_WRITE, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(h)

	cTime := windows.NsecToFiletime(creationTime.UnixNano())

	return windows.SetFileTime(h, &cTime, nil, nil)
}

func getCreationTime(filePath string) (time.Time, error) {
	h, err := windows.CreateFile(windows.StringToUTF16Ptr(filePath), windows.GENERIC_READ, windows.FILE_SHARE_READ, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return time.Time{}, err
	}
	defer windows.CloseHandle(h)

	var cTime windows.Filetime
	if err := windows.GetFileTime(h, &cTime, nil, nil); err != nil {
		return time.Time{}, err
	}

	return time.Unix(0, cTime.Nanoseconds()), nil
}
