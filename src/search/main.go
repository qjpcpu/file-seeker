package main

import (
    "bufio"
    "errors"
    "fmt"
    "fu"
    "github.com/voxelbrain/goptions"
    "log"
    "os"
    "strings"
)

func getSameLineAround(keyword string, i int, file *os.File, cursor int64, curline string) []string {
    current := cursor
    all := []string{curline}
    for {
        position, text, err := fu.Prevline(current, file)
        if err != nil || position == current {
            break
        }
        target := text
        if i > 0 {
            tokens := strings.Split(target, " ")
            target = tokens[i-1]
        }
        if target != keyword {
            break
        }
        all = append([]string{text}, all...)
        current = position
    }
    current = cursor
    for {
        position, text, err := fu.Nextline(current, file)
        if err != nil || position == current {
            break
        }
        target := text
        if i > 0 {
            tokens := strings.Split(target, " ")
            target = tokens[i-1]
        }
        if target != keyword {
            break
        }
        all = append(all, text)
        current = position
    }
    return all
}
func find(keyword string, i int, file *os.File) ([]string, error) {
    fi, _ := file.Stat()
    _high, _, _ := fu.Getline(fi.Size()-1, file)
    low, high := int64(0), int64(_high)
    mid := (low + high) / 2
    for low <= high {
        mid = (low + high) / 2
        mid, line, _ := fu.Getline(mid, file)
        target := line
        if i > 0 {
            tokens := strings.Split(target, " ")
            target = tokens[i-1]
        }
        if target == keyword {
            all := getSameLineAround(keyword, i, file, mid, line)
            return all, nil
        } else if target < keyword {
            next, _, err := fu.Nextline(mid, file)
            if err != nil {
                break
            }
            low = next
        } else {
            prev, _, err := fu.Prevline(mid, file)
            if err != nil {
                break
            }
            high = prev
        }
        if low == mid && mid == high {
            break
        }
    }
    return []string{}, errors.New("Can't found " + keyword)
}

func findAll(keyword string, i int, files []*os.File) {
    size := len(files)
    sig := make(chan string, size)
    for _, file := range files {
        go func(k string, index int, f *os.File, s chan string) {
            list, err := find(k, index, f)
            if err == nil {
                for _, text := range list {
                    fmt.Printf("%v: %v\n", f.Name(), text)
                }
            }
            sig <- f.Name()
        }(keyword, i, file, sig)
    }
    for j := 0; j < size; j++ {
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
    // Open file and db handle
    var files []*os.File
    for _, name := range options.Filename {
        file, err := os.Open(name)
        if err != nil {
            log.Fatal(err)
        }
        defer file.Close()
        files = append(files, file)
    }
    if options.Keyword != "" {
        findAll(options.Keyword, options.SearchIndex, files)
        return
    }
    kfile, err := os.Open(options.KeywordFile)
    if err != nil {
        log.Fatal(err)
    }
    defer kfile.Close()
    scanner := bufio.NewScanner(kfile)
    for scanner.Scan() {
        findAll(scanner.Text(), options.SearchIndex, files)
    }
}
