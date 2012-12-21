package riak

type Client struct {
    addr string
}

func New(addr string) (client *Client) {
    return &Client{addr: addr}
}

func (client *Client) Bucket(bucketname string) (bucket *Bucket) {
    return &Bucket{client: client, Name: bucketname}
}

func (client *Client) getConnection() (conn *connection) {
    return newConnection(client.addr)
}

func (client *Client) releaseConnection(conn *connection) {
    conn.Close()
}