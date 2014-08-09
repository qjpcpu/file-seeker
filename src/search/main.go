package main

import (
    "bufio"
    "errors"
    "fmt"
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/voxelbrain/goptions"
    "log"
    "os"
    "path/filepath"
    "strconv"
    "strings"
)

func buildIndex(filename string) {
    filename, _ = filepath.Abs(filename)
    file, err := os.Open(filename)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()
    index_path := getIndexDb(filename)
    db, err := leveldb.OpenFile(index_path, nil)
    if err != nil {
        log.Fatal("Build index failed", err)
    }
    defer db.Close()
    scanner := bufio.NewScanner(file)
    var i int64
    offset := 0
    batch := new(leveldb.Batch)
    for scanner.Scan() {
        i += 1
        line := scanner.Bytes()
        // key is line number
        // value format is 'offset line_length'
        batch.Put([]byte(fmt.Sprintf("%v", i)), []byte(fmt.Sprintf("%v %v", offset, len(line))))

        offset += len(line) + 1
        if i%100000 == 0 {
            db.Write(batch, nil)
            batch = new(leveldb.Batch)
        }
    }
    db.Write(batch, nil)
    db.Put([]byte("0"), []byte(fmt.Sprintf("%v", i)), nil)
}
func buildAllIndex(files ...string) {
    blist := []string{}
    for _, filename := range files {
        if _, err := os.Stat(getIndexDb(filename)); err != nil {
            blist = append(blist, filename)
        }
    }
    sig := make(chan string, 1)
    for _, filename := range blist {
        go func() {
            buildIndex(filename)
            sig <- filename
        }()
    }
    for i := 0; i < len(blist); i++ {
        <-sig
    }
}

func getIndexDb(filename string) string {
    return os.Getenv("HOME") + "/.file-seeker/" + filepath.Base(filename)
}
func getLine(num int, file *os.File, db *leveldb.DB) (string, error) {
    data, err := db.Get([]byte(strconv.Itoa(num)), nil)
    if err != nil || num < 1 {
        return "", errors.New("error in getline")
    }
    arr := strings.Split(string(data), " ")
    offset, _ := strconv.Atoi(arr[0])
    linelen, _ := strconv.Atoi(arr[1])
    line := make([]byte, linelen)
    file.ReadAt(line, int64(offset))
    return string(line), nil
}
func getSameLineAround(keyword string, i int, file *os.File, db *leveldb.DB, mid int, midline string) ([]int, []string) {
    cursor := mid - 1
    index_arr := []int{mid}
    line_arr := []string{midline}
    for {
        line, err := getLine(cursor, file, db)
        if err != nil {
            break
        }
        _t := line
        if i > 0 {
            tokens := strings.Split(line, " ")
            _t = tokens[i-1]
        }
        if _t == keyword {
            index_arr = append([]int{cursor}, index_arr...)
            line_arr = append([]string{line}, line_arr...)
            cursor -= 1
        } else {
            break
        }
    }
    cursor = mid + 1
    for {
        line, err := getLine(cursor, file, db)
        if err != nil {
            break
        }
        _t := line
        if i > 0 {
            tokens := strings.Split(line, " ")
            _t = tokens[i-1]
        }
        if _t == keyword {
            index_arr = append(index_arr, cursor)
            line_arr = append(line_arr, line)
            cursor += 1
        } else {
            break
        }
    }
    return index_arr, line_arr
}
func find(keyword string, i int, file *os.File, db *leveldb.DB) (int, string, error) {
    data, _ := db.Get([]byte("0"), nil)
    _high, _ := strconv.Atoi(string(data))
    low, high := 1, _high
    mid := (low + high) / 2
    for low <= high {
        mid = (low + high) / 2
        line, _ := getLine(mid, file, db)
        target := line
        if i > 0 {
            tokens := strings.Split(target, " ")
            target = tokens[i-1]
        }
        if target == keyword {
            return mid, line, nil
        } else if target < keyword {
            low = mid + 1
        } else {
            high = mid - 1
        }
    }
    return 0, "", errors.New("Can't found " + keyword)
}

func findAll(keyword string, index int, files []*os.File, dbs []*leveldb.DB) {
    sig := make(chan string, len(files))
    for i := 0; i < len(files); i++ {
        go func(i int) {
            name := files[i].Name()
            if id, line, err := find(keyword, index, files[i], dbs[i]); err == nil {
                fmt.Printf("%v:%v: %v\n", name, id, line)
            }
            sig <- name
        }(i)
    }
    for i := 0; i < len(files); i++ {
        <-sig
    }
}

type SearchFlags struct {
    SearchIndex int                `goptions:"-i,  description='search index number'"`
    Keyword     string             `goptions:"-k,  description='search keyword'"`
    KeywordFile string             `goptions:"-f,  description='search keyword'"`
    Filename    goptions.Remainder `goptions:"description='filename'"`
    Help        goptions.Help      `goptions:"-h,--help, description='Show this help'"`
}

func main() {
    options := SearchFlags{
        SearchIndex: 0,
    }
    goptions.ParseAndFail(&options)
    if options.Keyword == "" && options.KeywordFile == "" {
        log.Fatal("NO keyword")
    }
    if len(options.Filename) == 0 {
        log.Fatal("No table files")
    }
    buildAllIndex(options.Filename...)
    // Open file and db handle
    var files []*os.File
    var dbs []*leveldb.DB
    for _, name := range options.Filename {
        index_path := getIndexDb(name)
        db, err1 := leveldb.OpenFile(index_path, nil)
        file, err2 := os.Open(name)
        if err1 != nil || err2 != nil {
            log.Fatal(err1, err2)
        }
        defer db.Close()
        defer file.Close()
        files = append(files, file)
        dbs = append(dbs, db)
    }
    if options.Keyword != "" {
        findAll(options.Keyword, options.SearchIndex, files, dbs)
        return
    }
    kfile, err := os.Open(options.KeywordFile)
    if err != nil {
        log.Fatal(err)
    }
    defer kfile.Close()
    scanner := bufio.NewScanner(kfile)
    for scanner.Scan() {
        findAll(scanner.Text(), options.SearchIndex, files, dbs)
    }
}
