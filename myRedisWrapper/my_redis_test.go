package myRedisWrapper

import (
    "testing"
    redis "gopkg.in/redis.v5"
)

func TestPush(t *testing.T){
    client := redis.NewClient(&redis.Options{
        Addr: "127.0.0.1:6379",
    })
    red := NewMyRedis()
    value := "xml_string"
    red.Push_XML(value);

    // Check to make sure the string got stored
    result, err := client.LIndex(XML_KEY, 0).Result()
    if err != nil {
        t.Error("Could not fetch from redis")
        return
    }
    if result != value {
        t.Error("Expected", value, "got", result)
    }
}

func TestTrim(t *testing.T){
    client := redis.NewClient(&redis.Options{
        Addr: "127.0.0.1:6379",
    })

    red := NewMyRedis()
    value := "xml_string"
    i := int64(0)
    err := red.Push_XML(value)
    i++
    if err != nil {
        t.Error("my_redis couldn't push the xml")
        return
    }
    err = red.Push_XML(value)
    i++
    if err != nil {
        t.Error("my_redis couldn't push the xml")
        return
    }
    err = red.Push_XML(value)
    i++
    if err != nil {
        t.Error("my_redis couldn't push the xml")
        return
    }
    err = red.Push_XML(value)
    i++
    if err != nil {
        t.Error("my_redis couldn't push the xml")
        return
    }
    err = red.Push_XML(value)
    if err != nil {
        t.Error("my_redis couldn't push the xml")
        return
    }
    red.Trim()

    timeLength, err2 := client.LLen(TIME_KEY).Result()
    if err2 != nil {
        t.Error("Could not fetch from redis")
        t.Error(err2)
        return
    }
    if timeLength != 1 {
        t.Error("Expected:", 1, "Actual:", timeLength)
    }
}


func TestCheckLastTime(t *testing.T){
    client := redis.NewClient(&redis.Options{
        Addr: "127.0.0.1:6379",
    })

    red := NewMyRedis()
    value := "xml_string"
    maxVal := int64(99)
    err := red.Push_XML(value)
    if err != nil {
        t.Error("my_redis couldn't push the xml")
        return
    }
    err = red.Push_XML(value)
    if err != nil {
        t.Error("my_redis couldn't push the xml")
        return
    }
    err = red.Push_XML(value)
    if err != nil {
        t.Error("my_redis couldn't push the xml")
        return
    }
    err = red.Push_XML(value)
    if err != nil {
        t.Error("my_redis couldn't push the xml")
        return
    }
    err = red.Push_XML(value)
    if err != nil {
        t.Error("my_redis couldn't push the xml")
        return
    }
    red.Trim()

    timeLength, err2 := client.LLen(TIME_KEY).Result()
    if err2 != nil {
        t.Error("Could not fetch from redis")
        t.Error(err2)
        return
    }
    if timeLength != 1 {
        t.Error("Expected:", 1, "Actual:", timeLength)
    }

    theTime := red.GetLastTime()
    if theTime != maxVal {
        t.Error("Expected:", maxVal, "Actual:", theTime)
    }
}
