package store_redis

import (
    "bytes"
    "fmt"
    "github.com/DGHeroin/redcon"
    "github.com/DGHeroin/vault/store"
    "log"
    "net"
    "strconv"
    "strings"
)

func Serve(store *store.Store, ln net.Listener) error {
    var ps redcon.PubSub
    accept := func(conn redcon.Conn, cmd redcon.Command) {
        defer func() {
            if e := recover(); e != nil {
                conn.WriteError("ERR  '" + fmt.Sprint(e) + "'")
            }
        }()
        switch strings.ToLower(string(cmd.Args[0])) {
        default:
            fmt.Println(string(cmd.Args[0]))
            conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
        case "publish":
            // Publish to all pub/sub subscribers and return the number of
            // messages that were sent.
            if len(cmd.Args) != 3 {
                conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
                return
            }
            count := ps.Publish(string(cmd.Args[1]), string(cmd.Args[2]))
            conn.WriteInt(count)
        case "subscribe", "psubscribe":
            // Subscribe to a pub/sub channel. The `Psubscribe` and
            // `Subscribe` operations will detach the connection from the
            // event handler and manage all network I/O for this connection
            // in the background.
            if len(cmd.Args) < 2 {
                conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
                return
            }
            command := strings.ToLower(string(cmd.Args[0]))
            for i := 1; i < len(cmd.Args); i++ {
                if command == "psubscribe" {
                    ps.Psubscribe(conn, string(cmd.Args[i]))
                } else {
                    ps.Subscribe(conn, string(cmd.Args[i]))
                }
            }
        case "detach":
            conn2 := conn.Detach()
            log.Printf("connection has been detached")
            go func() {
                defer func() {
                    _ = conn2.Close()
                }()
                conn2.WriteString("OK")
                _ = conn2.Flush()
            }()
        case "ping":
            conn.WriteString("PONG")
        case "quit":
            conn.WriteString("OK")
            _ = conn.Close()
        case "set":
            if len(cmd.Args) != 3 {
                conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
                return
            }
            if err := store.Put(cmd.Args[1], cmd.Args[2]); err != nil {
                conn.WriteError("ERR '" + err.Error() + "'")
            } else {
                conn.WriteString("OK")
            }

        case "get":
            if len(cmd.Args) != 2 {
                conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
                return
            }
            if val, err := store.Get(cmd.Args[1]); err != nil {
                conn.WriteError("ERR '" + err.Error() + "'")
            } else {
                if val == nil {
                    conn.WriteNull()
                } else {
                    conn.WriteBulk(val)
                }
            }
        case "del":
            if len(cmd.Args) != 2 {
                conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
                return
            }
            if err := store.Del(cmd.Args[1]); err != nil {
                conn.WriteError("ERR '" + err.Error() + "'")
            } else {
                conn.WriteString("OK")
            }
        case "config":
            if len(cmd.Args) != 3 {
                conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
                return
            }
            // This simple (blank) response is only here to allow for the
            // redis-benchmark command to work with this example.
            conn.WriteArray(2)
            conn.WriteBulk(cmd.Args[2])
            conn.WriteBulkString("")
        case "type":
            conn.WriteString("string")
        case "keys":
            if len(cmd.Args) != 2 {
                conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
                return
            }
            var (
                k   [][]byte
                v   [][]byte
                err error
            )
            if bytes.Compare(cmd.Args[1], []byte("*")) == 0 {
                err = store.Range(nil, nil, func(key []byte, value []byte) bool {
                    k = append(k, key)
                    v = append(v, value)
                    return true
                })
            } else {
                err = store.RangePrefix(cmd.Args[1], func(key []byte, value []byte) bool {
                    k = append(k, key)
                    v = append(v, value)
                    return true
                })
            }
            if err != nil {
                conn.WriteError("ERR '" + err.Error() + "'")
                return
            }

            conn.WriteArray(len(k))
            for i := 0; i < len(k); i++ {
                conn.WriteString(string(k[i]))
            }
        case "scan":
            var (
                match  string
                cursor int
                count  int

                keys []string
            )
            sz := len(cmd.Args)
            if sz%2 != 0 {
                conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
                return
            }
            for i := 0; i < sz; i += 2 {
                t := string(cmd.Args[i])
                val := string(cmd.Args[i+1])
                switch strings.ToLower(t) {
                case "scan":
                    cursor, _ = strconv.Atoi(val)
                case "match":
                    match = val
                case "count":
                    count, _ = strconv.Atoi(val)
                }
            }
            curCursor := 0
            matchN := 0
            isBreakByCount := false

            if strings.Count(match, "*") == 1 && strings.HasSuffix(match, "*") {
                prefix := strings.ReplaceAll(match, "*", "")
                err := store.RangePrefix([]byte(prefix), func(k, value []byte) bool {
                    if match != "" {
                        if stringGlob(match, string(k)) {
                            if cursor > 0 && curCursor < cursor {
                                curCursor++
                                return true
                            }
                            keys = append(keys, string(k))
                            matchN++
                        } else {
                            // 不匹配
                            return true
                        }
                    } else {
                        if cursor > 0 && curCursor < cursor {
                            curCursor++
                            return true
                        }

                        keys = append(keys, string(k))
                        matchN++
                    }
                    curCursor++
                    // check limit
                    if count != 0 {
                        if matchN >= count {
                            isBreakByCount = true
                            return false
                        }
                    }
                    return true
                })
                if err != nil {
                    conn.WriteError("ERR '" + err.Error() + "'")
                    return
                }
            } else { // match ?
                err := store.Range(nil, nil, func(key, _ []byte) bool {
                    k := string(key)
                    if match != "" {
                        if stringGlob(match, k) {
                            if cursor > 0 && curCursor < cursor {
                                curCursor++
                                return true
                            }
                            keys = append(keys, k)
                            matchN++
                        } else {
                            // 不匹配
                            return true
                        }
                    } else {
                        if cursor > 0 && curCursor < cursor {
                            curCursor++
                            return true
                        }

                        keys = append(keys, k)
                        matchN++
                    }
                    curCursor++
                    // check limit
                    if count != 0 {
                        if matchN >= count {
                            isBreakByCount = true
                            return false
                        }
                    }
                    return true
                })
                if err != nil {
                    conn.WriteError("ERR '" + err.Error() + "'")
                    return
                }
                if !isBreakByCount {
                    curCursor = 0
                }
            }
            if len(keys) == 0 {
                conn.WriteArray(2)
                conn.WriteBulkString(fmt.Sprint(0))
                conn.WriteArray(matchN)
                for _, key := range keys {
                    conn.WriteBulkString(key)
                }
                return
            }
            conn.WriteArray(2)
            conn.WriteString(fmt.Sprint(curCursor))
            conn.WriteArray(matchN)
            for _, key := range keys {
                conn.WriteBulkString(key)
            }
        }
    }

    return redcon.Serve(ln, accept,
        func(conn redcon.Conn) bool {
            // Use this function to accept or deny the connection.
            // log.Printf("accept: %s", conn.RemoteAddr())
            return true
        },
        func(conn redcon.Conn, err error) {
            // This is called when the connection has been closed
            // log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
        })
}

func stringGlob(pattern, subj string) bool {
    if pattern == "" {
        return subj == pattern
    }

    if pattern == "*" {
        return true
    }

    parts := strings.Split(pattern, "*")

    if len(parts) == 1 {
        return subj == pattern
    }

    leadingGlob := strings.HasPrefix(pattern, "*")
    trailingGlob := strings.HasSuffix(pattern, "*")
    end := len(parts) - 1

    for i := 0; i < end; i++ {
        idx := strings.Index(subj, parts[i])

        switch i {
        case 0:
            if !leadingGlob && idx != 0 {
                return false
            }
        default:
            if idx < 0 {
                return false
            }
        }

        subj = subj[idx+len(parts[i]):]
    }

    return trailingGlob || strings.HasSuffix(subj, parts[end])
}
