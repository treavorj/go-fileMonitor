package fileMonitor

type Publisher interface {
	Publish(result [][]byte, id []string) error
}
