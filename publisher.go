package fileMonitor

type Publisher interface {
	Publish(dir *Dir, result [][]byte, id []string) error
}
