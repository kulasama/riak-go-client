package riak

import (
    "log"
)


func debugf(format string, v ...interface{}) {
    log.Printf("debug: " + format, v...)
}