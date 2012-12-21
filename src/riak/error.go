package riak

import (
    "errors"
)

var NotFound = errors.New("NotFound")
var AlreadyExists = errors.New("AlreadyExists")