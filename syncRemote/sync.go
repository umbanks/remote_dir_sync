package syncRemote

import (
    "golang.org/x/net/html"
    "net/http"
    "io"
    "io/ioutil"
    "log"
    "myRedisWrapper"
	"os"
    "path/filepath"
    "strings"
    "strconv"
	"archive/zip"
    // "time"
)

type gotXML struct {
    xml string
    timeStamp int64
}

// Unzip the archive location to the target directory
func unzip(archive string, target string) error {
    reader, err := zip.OpenReader(archive)
    if err != nil {
        return err
    }

    if err := os.MkdirAll(target, 0755); err != nil {
        return err
    }

    for _, file := range reader.File {
        path := filepath.Join(target, file.Name)
        if file.FileInfo().IsDir() {
            os.MkdirAll(path, file.Mode())
            continue
        }

        fileReader, err := file.Open()
        if err != nil {
            return err
        }
        defer fileReader.Close()

        targetFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
        if err != nil {
            return err
        }
        defer targetFile.Close()

        if _, err := io.Copy(targetFile, fileReader); err != nil {
            return err
        }
    }

    return nil
}

// Function to grab the zip file from the remote directory and save it
func goGetter(path string, zipDest string) error {
    log.Println("Downloading", path + zipDest)
    resp, err := http.Get(path + zipDest)

    if err != nil {
        log.Fatal(err)
        return err
    }
	defer resp.Body.Close()

    //open a file for writing
    file, err := os.Create(zipDest)
    if err != nil {
        log.Fatal(err)
        return err
    }
    // Use io.Copy to just dump the response body to the file. This supports huge files
    _, err = io.Copy(file, resp.Body)
    if err != nil {
        log.Fatal(err)
        return err
    }
    file.Close()
    return nil
}

// runner is the function that is executed as a goroutine, accepting timestamps as filenames
// and pushing the parsed xml files to the channel
func runner(path string, timeStampPop <-chan int64, xmlStringPush chan<- gotXML){
    for timeStamp := range timeStampPop {
        dest := strconv.FormatInt(timeStamp, 10)
        zipDest := dest + ".zip"
        goGetter(path, zipDest)

        log.Println("Unzipping file", dest)
        unzip(zipDest, dest)


        // Iterate over files in output dir
        files, _ := ioutil.ReadDir(dest)
        log.Printf("%d got %d files", timeStamp, len(files));
        i := 0
        for _, f := range files {
            fileLoaded := dest + "/" + f.Name()
            b, err := ioutil.ReadFile(fileLoaded)
            if err != nil {
                log.Println(err)
                break
            }
            xmlStr := string(b)
            xmlStringPush <- gotXML{xml: xmlStr, timeStamp: timeStamp}
            i++
        }
        log.Printf("%d put %d files in redis", timeStamp, i)
        os.RemoveAll(dest)
        os.RemoveAll(zipDest)
    }
}

// Sync takes a path to a remote directory and synchronizes the xml 
// files in the zipped directories contained therein in the local redis DB.
func Sync(path string) {
    // Prepare for asynchronous calls to the directory
    nameSlice  := getFileNames(path)
    namePush   := make(chan int64, len(nameSlice))
    xmlPop     := make(chan gotXML)

    mRedis := myRedisWrapper.NewMyRedis()
    latestTime := mRedis.GetLastTime()

    // 5 is arbitrary, it wouldn't be difficult to tune this to an optimal
    // value, or even make it dynamic based on memory, network traffic, cpu usage, etc.
    for i := 0; i < 5; i++ {
        go runner(path, namePush, xmlPop)
    }

    log.Printf("Found %d files for download", len(nameSlice))
    for _, theTime := range nameSlice {
        // We only want to fetch files that we haven't fetched before.
        if theTime > latestTime {
            namePush <- theTime
        } else {
            log.Println("Already downloaded", theTime)
        }
    }
    close(namePush)
    log.Println("Finished pushing files to chan")
    recievedTime := int64(-1)
    for response := range xmlPop {
        if response.timeStamp > recievedTime {
            recievedTime = response.timeStamp
            log.Println("xml_push recieved", recievedTime)
            mRedis.Trim()
        }
        mRedis.UpdateTime(response.timeStamp)
        mRedis.PushXML(response.xml)
    }

}

func getFileNames(path string) []int64 {
    log.Print("Getting data from manifest:", path)
    var retSlice []int64
    resp, err := http.Get(path)
    if err != nil {
        log.Fatal(err)
        return retSlice
    }
    z := html.NewTokenizer(resp.Body)
    for {
        tt := z.Next()
        switch{
        case tt == html.ErrorToken:
            return retSlice
        case tt == html.StartTagToken:
            t := z.Token()
            isAnchor := t.Data == "a"
            if isAnchor {
                for _, a := range t.Attr {
                    if a.Key == "href" {

                        stringTime := strings.Split(a.Val, ".")[0]
                        timeStamp, er := strconv.ParseInt(stringTime, 10, 64)
                        if er == nil {
                            retSlice = append(retSlice, timeStamp)
                        }
                    }
                }
            }
        }
    }
    return retSlice
}
