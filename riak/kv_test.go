package riak

import (
    "testing"
    "bytes"
)

func TestGetUnknownKey(t *testing.T) {
    client := New("127.0.0.1:8087")
    bucket := client.Bucket("bname")

    _, err := bucket.Get("unknown_key")
    if err == nil {
        t.Fatalf("Expected error on unknown key")
    }

    if err != NotFound {
        t.Fatalf("Expected NotFound error, actually: %v", err)
    }
}

func TestPutGetKey(t *testing.T) {
    client := New("127.0.0.1:8081")
    bucket := client.Bucket("bname")

    obj, err := bucket.New("new_key")
    if err != nil {
        t.Fatalf("Unexpected error: %v", err)
    }

    obj.Data = []byte("Hello World!")
    obj.ContentType = "text/plain"
    obj.Store()

    obj2, err := bucket.Get("new_key")
    if err != nil {
        t.Fatalf("Unexpected error: %v", err)
    }

    if !bytes.Equal(obj.Data, obj2.Data) {
        t.Fatalf("Expected equal Data")
    }

    if obj.ContentType != obj2.ContentType {
        t.Fatalf("Expected equal ContentType")   
    }
}
