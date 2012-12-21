package riak

import (
    "sync"
)

type Client struct {
    addr string
    mu sync.Mutex
    free_conns []*connection
}

func New(addr string) (client *Client) {
    return &Client{addr: addr}
}

func (client *Client) Bucket(bucketname string) (bucket *Bucket) {
    return &Bucket{client: client, Name: bucketname}
}

func (client *Client) getConnection() (conn *connection) {
    client.mu.Lock()
    defer client.mu.Unlock()

    if len(client.free_conns) != 0 {
        conn := client.free_conns[0]
        client.free_conns = client.free_conns[1:]
        return conn
    }

    return newConnection(client.addr)
}

func (client *Client) releaseConnection(conn *connection) {
    client.mu.Lock()
    defer client.mu.Unlock()

    client.free_conns = append(client.free_conns, conn)
}