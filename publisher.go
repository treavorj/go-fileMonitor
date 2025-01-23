package main

type Publisher interface {
	Publish(result [][]byte, id []string) error
}
