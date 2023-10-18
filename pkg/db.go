package db

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"unsafe"
)

type adress uint64

type KeysTable map[string]adress

type DataTable map[adress][]byte

const CELL_SIZE = 32
const KEY_MAX_SIZE = 16
const MAX_COUNT = 64

var _adress adress = 0

var keyTableSize uint = (KEY_MAX_SIZE + uint(unsafe.Sizeof(_adress))) * MAX_COUNT

var allocationTableSize uint = 8

var dataOffset = keyTableSize + allocationTableSize

type DB struct {
	Filename string

	m          sync.RWMutex
	f          *os.File
	allocation uint64
	keys       KeysTable
	data       DataTable
}

func New(filename string) DB {
	return DB{
		Filename:   filename,
		allocation: 0,
		keys:       make(KeysTable, MAX_COUNT),
		data:       make(DataTable, MAX_COUNT),
	}
}

func (db *DB) SetAllocation(alloc uint64) {
	db.allocation = alloc
}

func (db *DB) ReadAllocation() error {
	b := make([]byte, allocationTableSize)
	n, err := db.f.Read(b)
	if n != len(b) {
		return errors.New("can't read allocation bits")
	}
	if err != nil {
		return err
	}
	db.allocation = binary.BigEndian.Uint64(b)
	return nil
}

func (db *DB) WriteAllocation() error {
	b := make([]byte, allocationTableSize)
	binary.BigEndian.PutUint64(b, db.allocation)
	db.f.Seek(0, 0)
	n, err := db.f.Write(b)
	if n != len(b) {
		return errors.New("can't write allocation bits")
	}
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) ReadKeys() error {
	// db.f.Seek(8, 0)

	b := make([]byte, keyTableSize)
	n, err := db.f.ReadAt(b, int64(allocationTableSize))
	if n != len(b) {
		return errors.New("can't read key table")
	}
	if err != nil {
		return err
	}

	for i := 0; i < len(b); i += KEY_MAX_SIZE + 8 {
		kBytes := b[i : i+KEY_MAX_SIZE]
		adrBytes := b[i+KEY_MAX_SIZE : i+KEY_MAX_SIZE+8]

		key := string(trim(kBytes))
		adr := binary.LittleEndian.Uint64(adrBytes)

		db.keys[key] = adress(adr)
	}

	return nil
}

func (db *DB) WriteKeys() error {
	b := make([]byte, 0, keyTableSize)
	for k, adr := range db.keys {
		kBytes := []byte(k)
		if len(kBytes) > KEY_MAX_SIZE {
			return errors.New("key is too long")
		}
		if len(kBytes) < KEY_MAX_SIZE {
			kBytes = pad(kBytes, KEY_MAX_SIZE)
		}
		adrBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(adrBytes, uint64(adr))

		b = append(b, kBytes...)
		b = append(b, adrBytes...)
	}

	n, err := db.f.WriteAt(b, int64(allocationTableSize))
	if n != len(b) {
		return errors.New("can't write key table")
	}
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) ReadData() error {
	for _, adr := range db.keys {
		bytes := make([]byte, CELL_SIZE)
		n, err := db.f.ReadAt(bytes, int64(dataOffset)+int64(adr)*CELL_SIZE)
		if n != len(bytes) {
			return errors.New("can't read data table")
		}
		if err != nil {
			return err
		}
		db.data[adr] = trim(bytes)
	}
	return nil
}

func (db *DB) WriteData() error {
	for adr, data := range db.data {
		if len(data) > CELL_SIZE {
			return errors.New("data is too long")
		}
		if len(data) < CELL_SIZE {
			data = pad(data, CELL_SIZE)
		}
		n, err := db.f.WriteAt(data, int64(dataOffset)+int64(adr)*CELL_SIZE)
		if n != len(data) {
			return errors.New("can't write data")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	err := db.ReadKeys()
	if err != nil {
		return nil, err
	}

	err = db.ReadData()
	if err != nil {
		return nil, err
	}

	return db.data[db.keys[key]], nil
}

func (db *DB) Set(key string, value []byte) error {
	db.m.Lock()
	defer db.m.Unlock()
	// finds nearest unallocated address
	var adr uint64
	var allocPos int64
	for allocPos = MAX_COUNT - 1; allocPos >= 0; allocPos-- {
		if (db.allocation>>allocPos)&1 == 0 {
			break
		}
	}

	// Allocate
	db.allocation |= (1 << allocPos)

	adr = MAX_COUNT - uint64(allocPos)
	// Add to keys map
	db.keys[key] = adress(adr)

	// Add data
	db.data[adress(adr)] = value

	if err := db.WriteAllocation(); err != nil {
		return err
	}
	if err := db.WriteKeys(); err != nil {
		return err
	}
	if err := db.WriteData(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Open() error {
	if _, err := os.Stat(db.Filename); errors.Is(err, os.ErrNotExist) {
		f, err := os.Create(db.Filename)
		if err != nil {
			return err
		}

		db.f = f
		return nil
	}

	f, err := os.OpenFile(db.Filename, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	db.f = f
	err = db.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) Close() error {
	return db.f.Close()
}

func (db *DB) Sync() error {
	err := db.ReadAllocation()
	if err != nil {
		return err
	}
	err = db.ReadKeys()
	if err != nil {
		return err
	}

	err = db.ReadData()
	if err != nil {
		return err
	}
	return nil
}

func pad(bytes []byte, size int) []byte {
	padding := make([]byte, size-len(bytes))
	return append(bytes, padding...)
}

func trim(bytes []byte) []byte {
	trimmed := make([]byte, 0, len(bytes))

	for _, b := range bytes {
		if b != 0x0 {
			trimmed = append(trimmed, b)
		}
	}

	for i := len(bytes) - 1; i <= 0; i-- {
		if bytes[i] != 0x0 {
			trimmed = bytes[0 : i+1]
		}
	}

	return trimmed
}
