package riak

import (
    "riak/riak_pb"
)

type Object struct {
    bucket *Bucket
    vclock []byte

    Key string
    ContentType string
    Data []byte
}

func (obj *Object) Store() (err error) {
    return obj.put()
}

func (obj *Object) put() (err error) {
    bucket := obj.bucket
    client := bucket.client

    req := &riak_pb.RpbPutReq{Bucket: []byte(bucket.Name), Key: []byte(obj.Key), Vclock: obj.vclock}
    req.Content = &riak_pb.RpbContent{Value: obj.Data, ContentType: []byte(obj.ContentType)}

    conn := client.getConnection()
    defer client.releaseConnection(conn)

    if err = conn.sendMessage("RpbPutReq", req); err != nil {
        return
    }

    resp := &riak_pb.RpbPutResp{}
    err = conn.recvMessage(resp)
    if err != nil {
        return
    }

    return
}

func (obj *Object) get() (err error) {
    bucket := obj.bucket
    client := bucket.client
    req := &riak_pb.RpbGetReq{Bucket: []byte(bucket.Name), Key: []byte(obj.Key)}

    // XXX: set IfModified if vclock available

    conn := client.getConnection()
    defer client.releaseConnection(conn)

    if err = conn.sendMessage("RpbGetReq", req); err != nil {
        return
    }

    resp := &riak_pb.RpbGetResp{}
    err = conn.recvMessage(resp)
    if err != nil {
        return
    }

    // XXX: check Unchanged

    if len(resp.Content) == 0 {
        return NotFound
    }

    obj.vclock = resp.Vclock

    content := resp.Content[0]
    obj.Data = content.Value
    obj.ContentType = string(content.ContentType)

    return
}
