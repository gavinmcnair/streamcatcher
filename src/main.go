package main

import (
    "bufio"
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "io/ioutil"
    "log"
    "net/http"
    "net/url"
    "os"
    "os/user"
    "strings"
    "sync"
    "time"
)

const maxFileSize = 4 * 1024 * 1024 * 1024 // 4GiB


type StreamConfig struct {
    URL              string `json:"url"`
    OutputFilePrefix string `json:"outputFilePrefix"`
    IsPlaylist       bool   `json:"isPlaylist,omitempty"`
}

func downloadSegment(url, filename string) {
    resolvedFilename := resolvePath(filename)
    log.Printf("Downloading segment from URL: %s to file: %s\n", url, resolvedFilename)

    resp, err := http.Get(url)
    if err != nil {
        log.Println("Error downloading segment:", err)
        return
    }
    defer resp.Body.Close()

    file, err := os.Create(resolvedFilename)
    if err != nil {
        log.Println("Error creating file:", err)
        return
    }
    defer file.Close()

    io.Copy(file, resp.Body)
}


func downloadFromPlaylist(playlistURL, outputFilePrefix string) {
    log.Printf("Downloading from playlist: %s\n", playlistURL)

    resp, err := http.Get(playlistURL)
    if err != nil {
        log.Println("Error fetching playlist:", err)
        return
    }
    defer resp.Body.Close()

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        log.Println("Error reading playlist:", err)
        return
    }

    baseURL, err := url.Parse(playlistURL)
    if err != nil {
        log.Fatalf("Error parsing base URL: %v", err)
    }

    scanner := bufio.NewScanner(bytes.NewReader(body))
    segmentNumber := 1
    for scanner.Scan() {
        line := strings.TrimSpace(scanner.Text())
        if !strings.HasPrefix(line, "#") && line != "" {
            absoluteSegmentURL := baseURL.ResolveReference(&url.URL{Path: line}).String()
            
            outputFile := fmt.Sprintf("%s_part%d.ts", outputFilePrefix, segmentNumber)
            downloadSegment(absoluteSegmentURL, outputFile)
            segmentNumber++
        }
    }
}



func downloadStream(streamConfig StreamConfig, wg *sync.WaitGroup) {
    defer wg.Done()

    if streamConfig.IsPlaylist || strings.HasSuffix(streamConfig.URL, ".m3u") || strings.HasSuffix(streamConfig.URL, ".m3u8") {
        downloadFromPlaylist(streamConfig.URL, streamConfig.OutputFilePrefix)
        return
    }

    client := &http.Client{}
    totalBytes := int64(0)
    partNumber := 1

    for {
        resp, err := client.Get(streamConfig.URL)
        if err != nil {
            log.Println("Error connecting to stream:", err)
            time.Sleep(10 * time.Second)
            continue
        }

        outputFile, err := os.Create(fmt.Sprintf("%s_part%d.ts", streamConfig.OutputFilePrefix, partNumber))
        if err != nil {
            log.Fatal(err)
        }

        buf := make([]byte, 4096)
        for {
            bytesRead, err := resp.Body.Read(buf)
            if bytesRead == 0 && (err == io.EOF || err == nil) {
                time.Sleep(time.Second)
                continue
            } else if err != nil {
                log.Println("Error reading from stream:", err, "| Bytes read:", totalBytes)
                break
            }

            totalBytes += int64(bytesRead)

            if totalBytes >= maxFileSize {
                outputFile.Close()
                partNumber++
                outputFile, err = os.Create(fmt.Sprintf("%s_part%d.ts", streamConfig.OutputFilePrefix, partNumber))
                if err != nil {
                    log.Fatal(err)
                }
                totalBytes = 0
            }

            _, err = outputFile.Write(buf[:bytesRead])
            if err != nil {
                log.Println("Error writing to file:", err)
            }
        }

        outputFile.Close()
        resp.Body.Close()

        time.Sleep(10 * time.Second)
    }
}

func main() {
    file, err := os.Open("streams.json")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    streamConfigs := []StreamConfig{}
    err = json.NewDecoder(file).Decode(&streamConfigs)
    if err != nil {
        log.Fatal(err)
    }

    var wg sync.WaitGroup

    for _, streamConfig := range streamConfigs {
        wg.Add(1)
        go downloadStream(streamConfig, &wg)
    }

    wg.Wait()
}

func resolvePath(path string) string {
    if strings.HasPrefix(path, "~/") {
        usr, err := user.Current()
        if err != nil {
            log.Fatalf("Error fetching the user's home directory: %v", err)
        }
        return strings.Replace(path, "~", usr.HomeDir, 1)
    }
    return path
}

