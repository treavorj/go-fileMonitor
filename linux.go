//go:build linux

package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

func SmbMount(username, password, server, shareName string) error {
	if shareName == "" {
		return fmt.Errorf("shareName cannot be blank")
	} else if server == "" {
		return fmt.Errorf("server cannot be blank")
	} else if username == "" {
		return fmt.Errorf("username cannot be blank")
	}

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

func SmbRemove(server, shareName string) error {
	if shareName == "" {
		return fmt.Errorf("shareName cannot be blank")
	} else if server == "" {
		return fmt.Errorf("server cannot be blank")
	}

	mountPoint := filepath.Join("/mnt", server, shareName)
	credentialsFile := fmt.Sprintf("/etc/smbcredentials_%s_%s", server, shareName)

	command := exec.Command("umount", mountPoint)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to unmount: %s, output: %s", err, string(output))
	}

	if err := os.Remove(credentialsFile); err != nil {
		return fmt.Errorf("failed to remove credentials file: %w", err)
	}

	if err := removeFstabEntry(server, shareName); err != nil {
		return fmt.Errorf("failed to remove fstab entry: %w", err)
	}

	if err := os.Remove(mountPoint); err != nil {
		return fmt.Errorf("failed to remove mount point: %w", err)
	}

	return nil
}

func removeFstabEntry(server, shareName string) error {
	fstabPath := "/etc/fstab"
	tempPath := fstabPath + ".tmp"

	input, err := os.Open(fstabPath)
	if err != nil {
		return fmt.Errorf("failed to open /etc/fstab: %w", err)
	}

	output, err := os.Create(tempPath)
	if err != nil {
		input.Close()
		return fmt.Errorf("failed to create temporary fstab file: %w", err)
	}

	// Filter out the line corresponding to the share
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, fmt.Sprintf("//%s/%s", server, shareName)) {
			continue
		}
		if _, err := output.WriteString(line + "\n"); err != nil {
			input.Close()
			output.Close()
			return fmt.Errorf("failed to write to temporary fstab file: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {
		input.Close()
		output.Close()
		return fmt.Errorf("error reading /etc/fstab: %w", err)
	}

	if err := input.Close(); err != nil {
		return fmt.Errorf("failed to close %s: %w", fstabPath, err)
	}
	if err := output.Close(); err != nil {
		return fmt.Errorf("failed to close %s: %w", tempPath, err)
	}

	if err := os.Rename(tempPath, fstabPath); err != nil {
		return fmt.Errorf("failed to update /etc/fstab: %w", err)
	}

	return nil
}
