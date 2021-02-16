package kvstore

import (
	"bytes"
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
	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)
	offset, err := WritePut(path, key, value)
	if err != nil {
		return err
	}

	k.Cache.Add(key, offset)

	return nil
}

func (k KvStore) Get(key string) (value string, err error) {
	offset, ok := k.Cache.Get(key)

	if ok != true {
		return "", errors.New("Unable to find offset in cache.")
	}

	off, check := offset.(int64)
	if !check {
		return "", errors.New("Offset is in inproper format.")
	}
	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)

	value, err = ReadGet(path, off)

	return value, err
}

func (k KvStore) Del(key string) error {
	offset, ok := k.Cache.Get(key)
	k.Cache.Remove(key)
	off, check := offset.(int64)
	if !check {
		return errors.New("Offset is in inproper format.")
	}

	if ok {
		log.Infof("Delete called for key %s, and offset %s", key, offset)
		path := filepath.Join(".", STORAGE_DIR)
		path = filepath.Join(path, STORAGE_FILE)

		return WriteDel(path, off)
	}

	return nil
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

	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, STORAGE_FILE)
	loadErr := LoadData(cache, path)

	if loadErr != nil {
		log.Fatal("Could not load data into offset cache.")
	}

	return &KvStore{cache}
}

func WritePut(filePath string, key string, value string) (offset int64, err error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		return 0, err
	}

	length, write_err := file.WriteString(fmt.Sprintf("%s,%s\n", key, value))
	fi, statErr := file.Stat()
	if statErr != nil {
		return 0, statErr
	}

	offset = fi.Size() - int64(length)
	file.Close()
	return offset, write_err
}

func ReadGet(filePath string, offset int64) (string, error) {
	storeFile, openErr := os.Open(filePath)

	if openErr != nil {
		return "", openErr
	}

	_, seekErr := storeFile.Seek(offset, 0)
	if seekErr != nil {
		return "", openErr
	}

	reader := csv.NewReader(storeFile)
	log.Infoln("Reading persistent file.")
	record, err := reader.Read()

	if err != nil {
		storeFile.Close()
		return "", err
	}

	value := record[1]
	storeFile.Close()

	return value, nil
}

func WriteDel(filePath string, offset int64) error {
	storeFile, openErr := os.Open(filePath)

	if openErr != nil {
		return openErr
	}

	_, seekErr := storeFile.Seek(offset, 0)
	if seekErr != nil {
		return openErr
	}

	reader := csv.NewReader(storeFile)
	log.Infoln("Reading persistent file.")
	record, err := reader.Read()

	if err != nil {
		storeFile.Close()
		return err
	}

	length := len(record[0]) + len(record[1]) + 2
	storeFile.Close()

	storeFile, openErr = os.OpenFile(filePath, os.O_WRONLY, 0644)

	if openErr != nil {
		return openErr
	}

	log.Infof("Read entry length %d", length)
	var newString string = ","
	for i := 2; i < length; i++ {
		newString += " "
	}
	newString += "\n"
	log.Infof("Created replacement string (empty) '%s'", newString)
	_, seekErr = storeFile.Seek(0, io.SeekStart)
	if seekErr != nil {
		storeFile.Close()
		return openErr
	}

	_, writeErr := storeFile.WriteAt([]byte(newString), offset)
	storeFile.Close()
	return writeErr
}

func LoadData(cache Cache, filePath string) (err error) {
	storeFile, openErr := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)

	if openErr != nil {
		return openErr
	}

	var buffer bytes.Buffer
	var position int64
	reader := io.TeeReader(storeFile, &buffer)
	csvReader := csv.NewReader(reader)

	log.Infoln("Reading persistent file into cache with offsets.")
	for {
		record, readErr := csvReader.Read()
		if readErr == io.EOF {
			log.Info("End of file reached.")
			break
		}

		if err != nil {
			err = readErr
			break
		}

		log.Infoln("Reading line bytes.")
		lineBytes, _ := buffer.ReadBytes('\n')
		log.Infoln("Read line bytes.")
		key := record[0]

		if key != "" {
			cache.Add(key, position)
		} else {
			log.Info("Empty line from delete detected, skipping.")
		}

		position += int64(len(lineBytes))
	}

	log.Infoln("Successfully Read persistent file into cache with offsets.")
	return err
}
