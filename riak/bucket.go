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
    return object, nil
}