package kvstore

import (
	"encoding/csv"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	STORAGE_DIR string = "storage"
	A_FILE      string = "a_record.csv"
	B_FILE      string = "b_record.csv"
	C_FILE      string = "c_record.csv"
	D_FILE      string = "d_record.csv"
	E_FILE      string = "e_record.csv"
	F_FILE      string = "f_record.csv"
	G_FILE      string = "g_record.csv"
	H_FILE      string = "h_record.csv"
	I_FILE      string = "i_record.csv"
	J_FILE      string = "j_record.csv"
	K_FILE      string = "k_record.csv"
	L_FILE      string = "l_record.csv"
	M_FILE      string = "m_record.csv"
	N_FILE      string = "n_record.csv"
	O_FILE      string = "o_record.csv"
	P_FILE      string = "p_record.csv"
	Q_FILE      string = "q_record.csv"
	R_FILE      string = "r_record.csv"
	S_FILE      string = "s_record.csv"
	T_FILE      string = "t_record.csv"
	U_FILE      string = "u_record.csv"
	V_FILE      string = "v_record.csv"
	W_FILE      string = "w_record.csv"
	X_FILE      string = "x_record.csv"
	Y_FILE      string = "y_record.csv"
	Z_FILE      string = "z_record.csv"
	ZERO_FILE   string = "0_record.csv"
	ONE_FILE    string = "1_record.csv"
	TWO_FILE    string = "2_record.csv"
	THREE_FILE  string = "3_record.csv"
	FOUR_FILE   string = "4_record.csv"
	FIVE_FILE   string = "5_record.csv"
	SIX_FILE    string = "6_record.csv"
	SEVEN_FILE  string = "7_record.csv"
	EIGHT_FILE  string = "8_record.csv"
	NINE_FILE   string = "9_record.csv"
)

type Store interface {
	Put(key string, value string) error
	Get(key string) (string, error)
	Del(key string) error
}

type KvStore struct {
	Cache              Cache
	LastLetterIndexMap map[string]string
}

func GetLastCharLower(key string) string {
	newVal := key[len(key)-1:]
	return strings.ToLower(newVal)
}

func (k KvStore) Put(key string, value string) error {
	k.Cache.Add(key, value)
	lastChar := GetLastCharLower(key)
	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, k.LastLetterIndexMap[lastChar])
	return WritePut(path, key, value)
}

func (k KvStore) Get(key string) (value string, err error) {
	val, ok := k.Cache.Get(key)

	if ok {
		value = val
		err = nil
	} else {
		lastChar := GetLastCharLower(key)
		path := filepath.Join(".", STORAGE_DIR)
		path = filepath.Join(path, k.LastLetterIndexMap[lastChar])
		value, err = ReadGet(path, key)
	}

	return value, err
}

func (k KvStore) Del(key string) error {
	k.Cache.Remove(key)
	lastChar := GetLastCharLower(key)
	path := filepath.Join(".", STORAGE_DIR)
	path = filepath.Join(path, k.LastLetterIndexMap[lastChar])
	return WriteDel(path, key)
}

func NewLastLetterIndexMap() map[string]string {
	var m map[string]string = make(map[string]string)
	m["a"] = A_FILE
	m["b"] = B_FILE
	m["c"] = C_FILE
	m["d"] = D_FILE
	m["e"] = E_FILE
	m["f"] = F_FILE
	m["g"] = G_FILE
	m["h"] = H_FILE
	m["i"] = I_FILE
	m["j"] = J_FILE
	m["k"] = K_FILE
	m["l"] = L_FILE
	m["m"] = M_FILE
	m["n"] = N_FILE
	m["o"] = O_FILE
	m["p"] = P_FILE
	m["q"] = Q_FILE
	m["r"] = R_FILE
	m["s"] = S_FILE
	m["t"] = T_FILE
	m["u"] = U_FILE
	m["v"] = V_FILE
	m["w"] = W_FILE
	m["x"] = X_FILE
	m["y"] = Y_FILE
	m["z"] = Z_FILE
	m["0"] = ZERO_FILE
	m["1"] = ONE_FILE
	m["2"] = TWO_FILE
	m["3"] = THREE_FILE
	m["4"] = FOUR_FILE
	m["5"] = FIVE_FILE
	m["6"] = SIX_FILE
	m["7"] = SEVEN_FILE
	m["8"] = EIGHT_FILE
	m["9"] = NINE_FILE

	return m
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

	cache, cErr := NewLruCache()

	if cErr != nil {
		log.Fatal("Could not create cache for kv store.")
	}

	var m map[string]string = NewLastLetterIndexMap()
	return &KvStore{cache, m}
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
