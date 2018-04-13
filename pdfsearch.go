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

func processorPDFDiscover(pdfFileChannel chan<- string) error {
	// TODO recursive
	files, err := ioutil.ReadDir(".")
	if err != nil {
		return err
	}
	for _, file := range files {
		if !file.IsDir() {
			if strings.HasSuffix(strings.ToLower(file.Name()), ".pdf") {
				path, err := filepath.Abs("./" + file.Name())
				if err != nil {
					return err
				}
				pdfFileChannel <- path
			}
		}
	}
	return nil
}

func processorPDFFiles(pdfFileChannel <-chan string, pdfPagesChannel chan<- *PDFPageProcessing, group *sync.WaitGroup) error {
	defer group.Done()
	for {
		pdfPath, ok := <-pdfFileChannel
		if !ok {
			break
		}
		cmd := exec.Command("pdftotext", pdfPath, "-")
		var buffer bytes.Buffer
		cmd.Stdout = &buffer
		cmd.Run()
		cmdOut := buffer.String()
		pages := strings.Split(cmdOut, "\f")
		for pi, page := range pages[:len(pages)-1] {
			pdfPagesChannel <- &PDFPageProcessing{
				path:    pdfPath,
				number:  pi + 1,
				content: page}
		}
	}
	return nil
}

func processorPDFPages(pdfPagesChannel <-chan *PDFPageProcessing, pdfMatchesChannel chan<- *PDFPageMatch, search string, group *sync.WaitGroup) error {
	defer group.Done()
	for {
		pdfPage, ok := <-pdfPagesChannel
		if !ok {
			break
		}
		var matchedLines []string
		lines := strings.Split(pdfPage.content, "\n")
		for _, line := range lines {
			// TODO case insensitive
			if strings.Contains(line, search) {
				matchedLines = append(matchedLines, line)
			}
		}
		if len(matchedLines) != 0 {
			pdfMatchesChannel <- &PDFPageMatch{
				path:         pdfPage.path,
				number:       pdfPage.number,
				matchedLines: matchedLines}
		}
	}
	return nil
}

func main() {
	// command line arguments (search string)
	if len(os.Args) < 2 {
		fmt.Println("Please provide a search term")
		os.Exit(1)
	}
	search := strings.Join(os.Args[1:], " ")

	// allocate channels
	pdfFileChannel := make(chan string, runtime.NumCPU())
	pdfPagesChannel := make(chan *PDFPageProcessing, 5*runtime.NumCPU())
	pdfMatchesChannel := make(chan *PDFPageMatch, 10*runtime.NumCPU())

	var wgFiles, wgPages, wgFinish sync.WaitGroup
	wgFiles.Add(runtime.NumCPU())
	wgPages.Add(runtime.NumCPU())
	wgFinish.Add(3)

	// start processing pipeline
	go func() {
		// step 1: PDF file discovery
		processorPDFDiscover(pdfFileChannel)
		close(pdfFileChannel)
		wgFinish.Done()
	}()
	for i := 0; i < runtime.NumCPU(); i++ {
		// step 2: PDF to text per page conversion
		go processorPDFFiles(pdfFileChannel, pdfPagesChannel, &wgFiles)
		// step 3: search with term on PDF pages
		go processorPDFPages(pdfPagesChannel, pdfMatchesChannel, search, &wgPages)
	}
	go func() {
		wgFiles.Wait()
		close(pdfPagesChannel)
		wgFinish.Done()
	}()
	go func() {
		wgPages.Wait()
		close(pdfMatchesChannel)
		wgFinish.Done()
	}()

	// wait for pipeline to finish
	wgFinish.Wait()

	// order matches
	fileMatches := make(map[string]*PDFFileMatch)
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
