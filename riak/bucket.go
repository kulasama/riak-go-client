package riak

type Bucket struct {
    client *Client
    Name string
}

// Get object
func (bucket *Bucket) Get(key string) (object *Object, err error) {
    object = &Object{bucket: bucket, Key: key}
    err = object.get()
    return
}

// Create new object
func (bucket *Bucket) New(key string) (object *Object, err error) {
    // XXX: check that object exists?
    // XXX: put new object?
    object = &Object{bucket: bucket, Key: key}
    return object, nil
}

// Check if object exists
func (bucket *Bucket) Exists(key string) (exists bool, err error) {
    object := &Object{bucket: bucket, Key: key}
    err = object.get()

    if err == NotFound {
        return false, nil
    } else if err == nil {
        return true, nil
    }

    return
}