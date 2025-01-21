//go:build linux

package fileMonitor

import (
	"fmt"
	"os"
	"os/exec"
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

func MountRemoteSMB(username, password, path string) error {
	if err := createCredentialsFile(username, password); err != nil {
		return fmt.Errorf("unable to create credentials: %w", err)
	}

	if err := addFstabEntry(path); err != nil {
		return fmt.Errorf("unable to create fstab entry: %w", err)
	}

	command := exec.Command(
		"mount",
		"-t cifs",
		"-o", "rw", fmt.Sprintf("username=%s,password=%s", username, password),
		path,
	)

	return command.Run()
}

func createCredentialsFile(username, password string) error {
	credentials := fmt.Sprintf("username=%s\npassword=%s\n", username, password)

	file, err := os.OpenFile("/etc/smbcredentials", os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("Failed to create /etc/smbcredentials: %w", err)
	}
	defer file.Close()

	if _, err = file.WriteString(credentials); err != nil {
		return fmt.Errorf("Failed to write to /etc/smbcredentials: %w", err)
	}
	return nil
}

func addFstabEntry(path string) error {
	entry := fmt.Sprintf("\n%s /mnt cifs credentials=/etc/smbcredentials 0 0\n", path)

	file, err := os.OpenFile("/etc/fstab", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("Failed to open /etc/fstab: %w", err)
	}
	defer file.Close()

	if _, err = file.WriteString(entry); err != nil {
		return fmt.Errorf("Failed to write to /etc/fstab: %w", err)
	}

	return nil
}
