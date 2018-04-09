package main

import (
	"os"
	"io/ioutil"
	"strings"
	"fmt"
	"runtime"
	"sync"
	"bytes"
	"path/filepath"
	"os/exec"
	"sort"
)

type PDFPageProcessing struct {
	path    string
	number  int
	content string
}

type PDFPageMatch struct {
	path         string
	number       int
	matchedLines []string
}

type PDFFileMatch struct {
	path         string
	matchedPages map[int][]string
}

func collectPDFFiles(pdfFileChannel chan<- os.FileInfo) {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		panic(err)
	}
	pdfFiles := make([]os.FileInfo, 0)
	for _, file := range files {
		if !file.IsDir() {
			if strings.HasSuffix(strings.ToLower(file.Name()), ".pdf") {
				pdfFiles = append(pdfFiles, file)
			}
		}
	}
	for _, pdfFile := range pdfFiles {
		pdfFileChannel <- pdfFile
	}
}

func processPDFFiles(pdfFileChannel <-chan os.FileInfo, pdfPagesChannel chan *PDFPageProcessing, pdfMatchesChannel chan<- *PDFPageMatch, search string) {
PROCESSLOOP:
	for {
		select {
		case pdfFile := <-pdfFileChannel:
			res, err := filepath.Abs("./" + pdfFile.Name())
			if err != nil {
				panic(err)
			}
			processPDFFile(res, pdfPagesChannel)
		case pdfPage := <-pdfPagesChannel:
			match := processPDFPage(pdfPage, search)
			if match != nil {
				pdfMatchesChannel <- match
			}
		default:
			break PROCESSLOOP
		}
	}
}

func processPDFFile(path string, pdfPagesChannel chan<- *PDFPageProcessing) {
	cmd := exec.Command("pdftotext", path, "-")
	var buffer bytes.Buffer
	cmd.Stdout = &buffer
	cmd.Run()
	stringding := buffer.String()
	pages := strings.Split(stringding, "\f")
	for pi, page := range pages[:len(pages)-1] {
		pdfPagesChannel <- &PDFPageProcessing{
			path:    path,
			number:  pi + 1,
			content: page}
	}
}

func processPDFPage(page *PDFPageProcessing, search string) *PDFPageMatch {
	var matchedLines []string
	lines := strings.Split(page.content, "\n")
	for _, line := range lines {
		// TODO case insensitive
		if strings.Contains(line, search) {
			matchedLines = append(matchedLines, line)
		}
	}
	if len(matchedLines) != 0 {
		return &PDFPageMatch{
			path:         page.path,
			number:       page.number,
			matchedLines: matchedLines}
	} else {
		return nil
	}
}

func main() {
	// command line arguments (search string)
	if len(os.Args) < 2 {
		fmt.Println("Please provide a search term")
		os.Exit(1)
	}

	search := strings.Join(os.Args[1:], " ")

	// allocate channels
	// TODO limits?
	pdfFileChannel := make(chan os.FileInfo, 1000)
	pdfPagesChannel := make(chan *PDFPageProcessing, 10000)
	pdfMatchesChannel := make(chan *PDFPageMatch, 10000)

	// collect PDF files from folder
	collectPDFFiles(pdfFileChannel)

	// process PDF files
	var wg sync.WaitGroup
	wg.Add(runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go func(i int) {
			processPDFFiles(pdfFileChannel, pdfPagesChannel, pdfMatchesChannel, search)
			wg.Done()
		}(i)
	}
	wg.Wait()

	// close channels
	close(pdfFileChannel)
	close(pdfPagesChannel)
	close(pdfMatchesChannel)

	// order matches
	fileMatches := make(map[string]*PDFFileMatch, 0)
	for match := range pdfMatchesChannel {
		elem, ok := fileMatches[match.path]
		if ok {
			elem.matchedPages[match.number] = match.matchedLines
		} else {
			matchMap := map[int][]string{}
			matchMap[match.number] = match.matchedLines
			elem = &PDFFileMatch{path: match.path, matchedPages: matchMap}
			fileMatches[match.path] = elem
		}
	}
	matchedFiles := make([]string, 0, len(fileMatches))
	for fileName := range fileMatches {
		matchedFiles = append(matchedFiles, fileName)
	}
	sort.Strings(matchedFiles)

	// print matches
	for _, fileName := range matchedFiles {
		matchedPagesInFile := &fileMatches[fileName].matchedPages
		fmt.Printf("=== %s (%d matched pages)\n", fileName, len(*matchedPagesInFile))
		matchedPages := make([]int, 0, len(*matchedPagesInFile))
		for page := range *matchedPagesInFile {
			matchedPages = append(matchedPages, page)
		}
		sort.Ints(matchedPages)
		for _, pageNumber := range matchedPages {
			for _, matchedLine := range (*matchedPagesInFile)[pageNumber] {
				fmt.Printf("Page %d: %s\n", pageNumber, matchedLine)
			}
		}
	}
}
