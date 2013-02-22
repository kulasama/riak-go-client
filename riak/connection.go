package riak

import (
    "encoding/binary"
    "bufio"
    "errors"
    "fmt"
    "net"
)

import (
    "github.com/kulasama/goprotobuf/proto"
)

import (
    "github.com/ivaxer/riak-go-client/riak_pb"
)

type connection struct {
    tcp_conn *net.TCPConn
    addr string
}

func newConnection(addr string) *connection {
    return &connection{addr: addr}
}

func (conn *connection) Close() {
    if conn.tcp_conn != nil {
        conn.tcp_conn.Close()
    }
}

func (conn *connection) sendMessage(name string, msg proto.Message) (err error) {
    if conn.tcp_conn == nil {
        if err = conn.connect(); err != nil {
            return
        }
    }

    msgcode, found := messageCodes[name]
    if !found {
        return fmt.Errorf("Unknown message name: '%s'", name)
    }

    msgbuf, err := proto.Marshal(msg)
    if err != nil {
        return
    }

    length := len(msgbuf) + 1

    // TODO: review it later

    w := bufio.NewWriter(conn.tcp_conn)

    if err = binary.Write(w, binary.BigEndian, uint32(length)); err != nil {
        return
    }

    if err = w.WriteByte(msgcode); err != nil {
        return
    }

    if _, err = w.Write(msgbuf); err != nil {
        return
    }

    if err = w.Flush(); err != nil {
        return
    }

    return
}

func (conn *connection) read(buf []byte) (err error) {
    have := 0

    for have != len(buf) {
        read, err := conn.tcp_conn.Read(buf[have:])
        if err != nil {
            return err
        }

        have += read
    }

    return
}

func (conn *connection) recvMessage(msg proto.Message) (err error) {
    if conn.tcp_conn == nil {
        return errors.New("recvMessage(): not connected")
    }

    var length uint32
    var msgcode byte

    if err = binary.Read(conn.tcp_conn, binary.BigEndian, &length); err != nil {
        debugf("conn.recvMessage(): read length error: %v", err)
        return
    }

    if err = binary.Read(conn.tcp_conn, binary.BigEndian, &msgcode); err != nil {
        debugf("conn.recvMessage(): read msgcode error: %v", err)
        return
    }

    msgbuf := make([]byte, length - 1)

    if len(msgbuf) == 0 {
        return
    }

    err = conn.read(msgbuf)
    if err != nil {
        debugf("conn.recvMessage(): read msg error: %v", err)
        return
    }

    if msgcode == messageCodes["RpbErrorResp"] {
        errResp := &riak_pb.RpbErrorResp{}
        err = proto.Unmarshal(msgbuf, errResp)
        if err != nil {
            debugf("conn.recvMessage(): proto.Unmarshal() error: %v", err)
            return
        }

        return fmt.Errorf("Riak error: %s", string(errResp.Errmsg))
    }

    err = proto.Unmarshal(msgbuf, msg)
    if err != nil {
        debugf("conn.recvMessage(): Can't unmarshal proto message: %v", err)
        return
    }

    return
}

func (conn *connection) connect() (err error) {
    if conn.tcp_conn != nil {
        return
    }

    raddr, err := net.ResolveTCPAddr("tcp", conn.addr)
    if err != nil {
        return
    }

    conn.tcp_conn, err = net.DialTCP("tcp", nil, raddr)
    if err != nil {
        return
    }

    err = conn.tcp_conn.SetNoDelay(true)

    return
}