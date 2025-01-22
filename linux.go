//go:build linux

package fileMonitor

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

func MountRemoteSMB(username, password, server, shareName string) error {
	credentialsFile, err := createCredentialsFile(username, password, server, shareName)
	if err != nil {
		return fmt.Errorf("unable to create credentials: %w", err)
	}

	mountPoint := filepath.Join("/mnt", server, shareName)
	if err := os.MkdirAll(mountPoint, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create mount point: %w", err)
	}

	if err := addFstabEntry(server, shareName, mountPoint, credentialsFile); err != nil {
		return fmt.Errorf("unable to create fstab entry: %w", err)
	}

	command := exec.Command(
		"mount",
		"-t", "cifs",
		"-o", fmt.Sprintf("credentials=%s,rw", credentialsFile),
		fmt.Sprintf("//%s/%s", server, shareName),
		mountPoint,
	)

	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount failed: %s, output: %s", err, string(output))
	}

	return nil
}

func createCredentialsFile(username, password, server, shareName string) (string, error) {
	credentials := fmt.Sprintf("username=%s\npassword=%s\n", username, password)

	file, err := os.OpenFile(fmt.Sprintf("/etc/smbcredentials_%s_%s", server, shareName), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return "", fmt.Errorf("Failed to create /etc/smbcredentials: %w", err)
	}
	defer file.Close()

	if _, err = file.WriteString(credentials); err != nil {
		return "", fmt.Errorf("Failed to write to /etc/smbcredentials: %w", err)
	}
	return file.Name(), nil
}

func addFstabEntry(server, shareName, mountPoint, credentialsFile string) error {
	entry := fmt.Sprintf("//%s/%s %s cifs credentials=%s 0 0\n",
		server, shareName,
		mountPoint,
		credentialsFile,
	)

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
