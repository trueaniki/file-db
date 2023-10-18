package main

import (
	DB "file-db/pkg"
	"log"
)

func main() {
	db := DB.New("test")
	db.Open()
	defer db.Close()
	err := db.Set("key1", []byte("data"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Set("key2", []byte("datadata"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Set("key3", []byte("datadatadata"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Set("key4", []byte("datadatadatadata"))
	if err != nil {
		log.Fatal(err)
	}
}
