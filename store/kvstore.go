package kvstore

import (
	"encoding/csv"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
)

const (
	STORAGE_DIR  string = "storage"
	STORAGE_FILE string = "data_records.csv"
)

type Store interface {
	Put(key string, value string) error
	Get(key string) (string, error)
	Del(key string) error
}

type KvStore struct {
	Cache Cache
}

func (k KvStore) Put(key string, value string) error {
	k.Cache.Add(key, value)
	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)
	return WritePut(path, key, value)
}

func (k KvStore) Get(key string) (value string, err error) {
	val, ok := k.Cache.Get(key)

	if ok {
		value = val
		err = nil
	} else {
		path := filepath.Join(".", STORAGE_DIR)
		path = filepath.Join(path, STORAGE_FILE)
		value, err = ReadGet(path, key)
	}

	return value, err
}

func (k KvStore) Del(key string) error {
	k.Cache.Remove(key)
	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)
	return WriteDel(path, key)
}

func NewKvStore() *KvStore {
	log.Info("Creating new Kv Store.")

	log.Info("Creating storage directory if does not exist.")
	newpath := filepath.Join(".", STORAGE_DIR)
	err := os.MkdirAll(newpath, os.ModePerm)

	if err != nil {
		log.Fatalf("Cannot create directory for storage at %s", STORAGE_DIR)
	}
	log.Info("Created storage directory.")

	cache, cErr := NewSimpleCache()

	if cErr != nil {
		log.Fatal("Could not create cache for kv store.")
	}

	return &KvStore{cache}
}

func WritePut(filePath string, key string, value string) error {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		return err
	}

	_, write_err := file.WriteString(fmt.Sprintf("%s,%s\n", key, value))
	file.Close()
	return write_err
}

func ReadGet(filePath string, key string) (string, error) {
	storeFile, openErr := os.Open(filePath)
	reader := csv.NewReader(storeFile)

	if openErr != nil {
		return "", openErr
	}

	log.Infoln("Reading persistent file.")
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			storeFile.Close()
			return "", err
		}

		k := record[0]
		v := record[1]

		if key == k {
			storeFile.Close()
			return v, nil
		}
	}

	return "", errors.New(fmt.Sprintf("Could not find value for key %s", key))
}

func WriteDel(filePath string, key string) error {

	tmpPath := filepath.Join(".", STORAGE_DIR)
	tmpPath = filepath.Join(tmpPath, "tmp_delete.csv")

	storeFile, openErr := os.Open(filePath)
	reader := csv.NewReader(storeFile)

	if openErr != nil {
		return openErr
	}

	log.Infoln("Reading persistent file for delete.")
	for {
		record, err := reader.Read()
		if err == io.EOF {
			log.Info("End of file reached.")
			break
		}

		if err != nil {
			return err
		}

		k := record[0]
		v := record[1]

		if key != k {
			writeErr := WritePut(tmpPath, k, v)

			if writeErr != nil {
				return writeErr
			}
		} else {
			log.Infof("Delete line found for key: %s", key)
		}
	}

	log.Infof("Delete completed for key: %s", key)
	storeFile.Close()
	return SwapFile(filePath, tmpPath)
}

func SwapFile(originalFilePath string, replacementFilePath string) error {
	log.Infof("Swapping file %s with file %s", originalFilePath, replacementFilePath)
	return os.Rename(replacementFilePath, originalFilePath)
}
