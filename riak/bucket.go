package riak

type Bucket struct {
    client *Client
    Name string
}

func (bucket *Bucket) Get(key string) (object *Object, err error) {
    object = &Object{bucket: bucket, Key: key}
    err = object.get()
    return
}

func (bucket *Bucket) New(key string) (object *Object, err error) {
    object = &Object{bucket: bucket, Key: key}

    err = object.get()
    if err == nil {
        debugf("bucket.New() failed: object %s/%s already exists", bucket.Name, key)
        return nil, AlreadyExists
    }

    if err != NotFound {
        debugf("bucket.New() failed: error while getting %s/%s key: %v", bucket.Name, key, err)
        return nil, err
    }



    err = object.put()
    return
}