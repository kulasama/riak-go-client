package riak

import (
    "testing"
    "bytes"
)

func setupClient(t *testing.T) *Client {
    return New("127.0.0.1:8087")
}

func TestGetUnknownKey(t *testing.T) {
    client := setupClient(t)
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
    client := setupClient(t)
    bucket := client.Bucket("bname")

    obj, err := bucket.New("new_key")
    if err != nil {
        t.Fatalf("Unexpected error: %v", err)
    }

    obj.Data = []byte("Hello World!")
    obj.ContentType = "text/plain"
    if err = obj.Store(); err != nil {
        t.Fatalf("Unexpected error: %v", err)
    }

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

func TestExists(t *testing.T) {
    client := setupClient(t)
    bucket := client.Bucket("bname")

    exists, err := bucket.Exists("unknown_key")
    if err != nil {
        t.Fatalf("Unexpected error: %v", err)
    }

    if exists {
        t.Fatalf("Unexpected 'unknown_key' existing")
    }

    obj, err := bucket.New("known_key")
    if err != nil {
        t.Fatalf("Unexpected error: %v", err)
    }

    obj.Data = make([]byte, 1)

    if err = obj.Store(); err != nil {
        t.Fatalf("Unexpected error: %v", err)
    }

    exists, err = bucket.Exists("known_key")
    if err != nil {
        t.Fatalf("Unexpected error: %v", err)
    }

    if !exists {
        t.Fatalf("Expected 'known_key' existing")
    }
}