package myRedisWrapper

import (
    // "fetch_dir"
    // "golang.org/x/net/html"
    redis "gopkg.in/redis.v5"
    "strconv"
    // "fmt"
    // "net/http"
    "log"
    // "sync_remote"
)

const (
    xmlKey = "NEWS_XML"
    timeKey = "XML_TIME"
)

var (
    latestTime     int64
)

/*
 * The object used to interface with the redis DB
 */
type myRedis struct {
    client      *redis.Client
    // The latest time to have been inserted into redis
}

// MyRedis provides the wrapper for the anonymous object myRedis
type MyRedis struct {
    myRedis
}

// NewMyRedis creates a myRedis object It isn't possible to create one any other way
func NewMyRedis() *MyRedis {
    client := redis.NewClient(&redis.Options{
        Addr: "127.0.0.1:6379",
    })
    mRedis := new(MyRedis)
    mRedis.client = client
    latestTime = mRedis.GetLastTime()
    return mRedis
}

/*
 * Convenience function to store the xml string,
 * along with the name of the file (the time stamp)
 * that accompanied it
 */
func (mRedis *myRedis) PushXML(xml string) error{
    _, err := mRedis.client.LPush(xmlKey, xml).Result()
    if err != nil {
        return err
    }

    // We want to guarantee that this doesn't get jacked,
    // it could cause serious problems
    return nil
}

// UpdateTime sends in a time, and if it is greater than the current time
// it updates it
func (mRedis *myRedis) UpdateTime(theTime int64) {
    if theTime > latestTime {
        latestTime = theTime
    }
}

/*
 * We need to call this after the program has saved all the xml files
 * so that we can preserve the latest time file
 */
func (mRedis *myRedis) Trim() error{
    // Easiest way to make sure the list is empty is to delete it
    _, err := mRedis.client.Del(timeKey).Result()

    // Insert the largest valued time
    val := strconv.FormatInt(latestTime, 10)
    _, err = mRedis.client.LPush(timeKey, val).Result()

    if err != nil {
        log.Fatal(err)
        return err
    }
    return nil
}

/*
 * Return the last time that was inserted in the db.
 * If it hasn't been stored to, it will return -1
 */

func (mRedis *myRedis) GetLastTime() int64 {
    result, err := mRedis.client.LIndex(timeKey, 0).Result()

    var i = int64(-1)
    i, err = strconv.ParseInt(result, 10, 64)
    if err != nil {
        return -1
    }
    return i
}
