package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type Result struct {
	err  error
	name string
}

func downloadPart(url string, dirname string, saved chan<- Result) {
	resp, err := http.Get(url)
	if err != nil {
		saved <- Result{err: err, name: url}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		saved <- Result{err: fmt.Errorf("bad status %d", resp.StatusCode), name: url}
		return
	}

	re := regexp.MustCompile(`seg-(\d+)`)
	m := re.FindStringSubmatch(url)
	if len(m) < 2 {
		saved <- Result{err: fmt.Errorf("bad segment number"), name: url}
		return
	}

	segment, err := strconv.Atoi(m[1])

	if err != nil {
		saved <- Result{err: err, name: url}
		return
	}

	name := fmt.Sprintf("%08d.ts", segment)

	file, err := os.Create(dirname + "/" + name)
	if err != nil {
		saved <- Result{err: err, name: url}
		return
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		saved <- Result{err: err, name: url}
		return
	}

	saved <- Result{err: nil, name: url}
}

func parseManifest(body io.ReadCloser, manifestUrl string) ([]string, error) {
	const prefix = "./"
	result := make([]string, 0)

	rootUrl := manifestUrl[:strings.LastIndex(manifestUrl, "/")]

	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, prefix) {
			result = append(result, rootUrl+"/"+strings.TrimPrefix(line, prefix))
		}
	}
	if err := scanner.Err(); err != nil {
		return result, err
	}

	return result, nil
}

func downloadVideo(manifestUrl string, name string, results chan<- Result) {
	resp, err := http.Get(manifestUrl)

	if err != nil {
		results <- Result{err, name}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		results <- Result{fmt.Errorf("bad status %d", resp.StatusCode), name}
		return
	}

	urls, err := parseManifest(resp.Body, manifestUrl)
	if err != nil {
		results <- Result{err, name}
		return
	}

	dirname := "videos/" + name
	err = os.Mkdir(dirname, 0777)
	if err != nil {
		results <- Result{err, name}
		return
	}

	partsResults := make(chan Result)

	for _, url := range urls {
		go downloadPart(url, dirname, partsResults)
	}

	for range urls {
		result := <-partsResults
		if result.err != nil {
			results <- Result{err, name}
			return
		}
	}

	results <- Result{nil, name}
	close(partsResults)
}

func createConcatFile(dirname string) (string, error) {
	path := filepath.Join(dirname, "concat.txt")
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	entries, err := os.ReadDir(dirname)
	if err != nil {
		return "", err
	}

	for _, entry := range entries {
		if !entry.IsDir() && !strings.HasSuffix(entry.Name(), ".txt") {
			line := fmt.Sprintf("file %s\n", entry.Name())

			_, err = file.WriteString(line)
			if err != nil {
				return "", err
			}
		}
	}

	return path, nil
}

func mergeParts(dirname string, result chan<- Result) {
	concatFilePath, err := createConcatFile(dirname)
	if err != nil {
		log.Println(err)
		result <- Result{err, dirname}
		return
	}

	cmd := exec.Command("ffmpeg",
		"-f", "concat", "-i", concatFilePath, "-c", "copy",
		filepath.Join(dirname+".mp4"),
	)

	err = cmd.Run()
	if err != nil {
		log.Println(err)
		result <- Result{err, dirname}
		return
	}

	result <- Result{nil, dirname}
}

func main() {
	var manifestsData []byte
	manifestsData, err := os.ReadFile("manifests.json")

	if err != nil {
		panic(err)
	}

	var nameToManifest map[string]string
	err = json.Unmarshal(manifestsData, &nameToManifest)
	if err != nil {
		panic(err)
	}

	downloadResults := make(chan Result)

	for name, url := range nameToManifest {
		go downloadVideo(url, name, downloadResults)
	}

	mergeResults := make(chan Result)
	downloadedNames := make([]string, 0)

	for range nameToManifest {
		downloadResult := <-downloadResults
		if downloadResult.err == nil {
			log.Println("downloaded:", downloadResult.name)
			downloadedNames = append(downloadedNames, downloadResult.name)
			dirname := "videos/" + downloadResult.name
			go mergeParts(dirname, mergeResults)
		} else {
			log.Println("could not download:", downloadResult.name, downloadResult.err)
		}
	}

	for range downloadedNames {
		mergeResult := <-mergeResults
		if mergeResult.err == nil {
			log.Println("merged:", mergeResult.name)
		} else {
			log.Println("could not merge:", mergeResult.name, mergeResult.err)
		}
	}

	close(downloadResults)
	close(mergeResults)
}
