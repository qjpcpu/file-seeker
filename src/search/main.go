package main

import (
	"bufio"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"github.com/voxelbrain/goptions"
	"errors"
	"strconv"
	"strings"
	"os"
	"path/filepath"
)

func buildIndex(filename string) {
	filename, _ = filepath.Abs(filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	index_path := os.TempDir() + "/" + filepath.Base(filename)
	db, err := leveldb.OpenFile(index_path, nil)
	if err != nil {
		log.Fatal("Build index failed", err)
	}
	defer db.Close()
	scanner := bufio.NewScanner(file)
	var i int64
	offset:=0
	batch := new(leveldb.Batch)
	for scanner.Scan() {
		i+=1
		line := scanner.Bytes()
		// key is line number
		// value format is 'offset line_length'
		batch.Put([]byte(fmt.Sprintf("%v",i)), []byte(fmt.Sprintf("%v %v", offset,len(line))))
		offset+=len(line)+1
		if i%1000000 == 0 {
			db.Write(batch, nil)
			batch = new(leveldb.Batch)
		}
	}
	db.Write(batch, nil)
	db.Put([]byte("0"),[]byte(fmt.Sprintf("%v",i)),nil)
}

func find(keyword string,i int, file *os.File,db *leveldb.DB) (int,string,error){
	data, _ := db.Get([]byte("0"), nil)
	_high,_:=strconv.Atoi(string(data))
	low,high:=1,_high
	mid:=(low+high)/2
	for ;low<=high;{
		mid=(low+high)/2
		data, _ = db.Get([]byte(strconv.Itoa(mid)), nil)
		arr:=strings.Split(string(data)," ")
		offset,_:=strconv.Atoi(arr[0])
		linelen,_:=strconv.Atoi(arr[1])
		line := make([]byte, linelen)
		file.ReadAt(line, int64(offset))
		target:=string(line)
		if i>0{
			tokens:=strings.Split(target," ")
			target=tokens[i-1]
		}
		if target==keyword{
			return mid,string(line),nil
		}else if target < keyword{
			low=mid+1
		}else{
			high=mid-1
		}
	}
	return 0,"",errors.New("Can't found "+keyword)
}
type SearchFlags struct {
	SearchIndex       int             `goptions:"-i,  description='search index number'"`
	Keyword       string             `goptions:"-k,  description='search keyword'"`
	Filename            goptions.Remainder `goptions:"description='filename'"`
	Help           goptions.Help      `goptions:"--help, description='Show this help'"`
}
func main() {
	options := SearchFlags{
		SearchIndex: 0,
	}
	goptions.ParseAndFail(&options)
	keyword,filename:=options.Keyword,options.Filename[0]
	if _,err:=os.Stat(os.TempDir()+"/"+filepath.Base(filename));err!=nil{
		buildIndex(filename)
	}
	index_path := os.TempDir() + "/" + filepath.Base(filename)
	db, _ := leveldb.OpenFile(index_path, nil)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	no,line,err:=find(keyword,options.SearchIndex,file,db)
	fmt.Println(no,line,err)
}
