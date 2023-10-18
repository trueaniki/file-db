package main

import (
	DB "file-db/pkg"
	"fmt"
	"log"
)

func main() {
	db := DB.New("test")
	db.Open()
	defer db.Close()

	value1, err := db.Get("key1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value1))

	value2, err := db.Get("key2")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value2))

	value3, err := db.Get("key3")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value3))

	value4, err := db.Get("key4")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value4))
}
