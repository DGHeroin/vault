package store

import (
    "encoding/binary"
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/errors"
    "github.com/syndtr/goleveldb/leveldb/util"
    "io"
)

type (
    Store struct {
        db *leveldb.DB
    }
    Putter interface {
        Put(key []byte, value []byte)
    }
    Deleter interface {
        Delete([]byte)
    }
)

func New(path string) (*Store, error) {
    db, err := leveldb.OpenFile(path, nil)
    if err != nil {
        return nil, err
    }
    s := &Store{
        db: db,
    }
    return s, nil
}
func (s *Store) Close() error {
    return s.db.Close()
}
func (s *Store) Get(key []byte) (result []byte, err error) {
    return s.db.Get(key, nil)
}
func (s *Store) Put(key, value []byte) error {
    return s.db.Put(key, value, nil)
}
func (s *Store) Del(key []byte) error {
    return s.db.Delete(key, nil)
}
func (s *Store) BatchPut(fn func(putter Putter)) error {
    batch := new(leveldb.Batch)
    fn(batch)
    return s.db.Write(batch, nil)
}
func (s *Store) BatchDel(fn func(del Deleter)) error {
    batch := new(leveldb.Batch)
    fn(batch)
    return s.db.Write(batch, nil)
}
func (s *Store) Range(start, limit []byte, fn func(key []byte, value []byte) bool) error {
    it := s.db.NewIterator(&util.Range{
        Start: start,
        Limit: limit,
    }, nil)
    defer it.Release()
    for it.Next() {
        if !fn(copyBytes(it.Key()), copyBytes(it.Value())) {
            break
        }
    }
    return it.Error()
}
func (s *Store) RangePrefix(prefix []byte, fn func(key []byte, value []byte) bool) error {
    it := s.db.NewIterator(util.BytesPrefix(prefix), nil)
    defer it.Release()
    for it.Next() {
        if !fn(copyBytes(it.Key()), copyBytes(it.Value())) {
            break
        }
    }
    return it.Error()
}
func (s *Store) GC(args ...[]byte) error {
    var (
        start []byte
        limit []byte
    )
    if len(args) == 2 {
        start = args[0]
        limit = args[1]
    }
    return s.db.CompactRange(util.Range{
        Start: start,
        Limit: limit,
    })
}
func (s *Store) Dump(w io.Writer) error {
    shot, err := s.db.GetSnapshot()
    if err != nil {
        return err
    }
    defer shot.Release()
    it := shot.NewIterator(nil, nil)
    defer it.Release()
    for it.Next() {
        k := it.Key()
        v := it.Value()
        header := make([]byte, 8)
        binary.BigEndian.PutUint32(header, uint32(len(k)))
        binary.BigEndian.PutUint32(header[4:], uint32(len(v)))
        if _, err := w.Write(header); err != nil {
            return err
        }
        if _, err := w.Write(k); err != nil {
            return err
        }
        if _, err := w.Write(v); err != nil {
            return err
        }
    }
    return nil
}
func (s *Store) Load(r io.Reader) error {
    var err error
    for {
        header := make([]byte, 8)
        if err = mustRead(r, header); err != nil {
            break
        }

        ks := binary.BigEndian.Uint32(header)
        vs := binary.BigEndian.Uint32(header[4:])

        k := make([]byte, ks)
        v := make([]byte, vs)
        if err = mustRead(r, k); err != nil {
            break
        }
        if err = mustRead(r, v); err != nil {
            break
        }
        if err = s.db.Put(k, v, nil); err != nil {
            break
        }
    }
    if err == io.EOF {
        err = nil
    }
    return err
}
func mustRead(r io.Reader, buf []byte) error {
    sz := len(buf)
    n, err := io.ReadFull(r, buf)
    if err != nil {
        return err
    }
    if n != sz {
        return errors.New("invalid read")
    }
    return nil
}
func copyBytes(data []byte) []byte {
    result := make([]byte, len(data))
    copy(result, data)
    return result
}
