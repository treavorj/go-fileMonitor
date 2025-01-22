//go:build windows

package fileMonitor

import (
	"bytes"
	"fmt"
	"os/exec"
	"syscall"
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

func MountRemoteSMB(username, password, server, shareName string) error {
	command := exec.Command(
		"net",
		"use", fmt.Sprintf(`\\%s\%s`, server, shareName),
		fmt.Sprintf(`/user:"%s"`, username),
		fmt.Sprintf(`"%s"`, password),
		"/persistent:yes",
	)

	var stderr bytes.Buffer
	command.Stderr = &stderr

	command.SysProcAttr = &syscall.SysProcAttr{
		HideWindow: true,
	}

	if err := command.Run(); err != nil {
		return fmt.Errorf("command failed: %s, stderr: %s", err, stderr.String())
	}

	return nil
}
