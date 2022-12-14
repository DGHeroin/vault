package se_vfs

import (
    "bufio"
    "bytes"
    "fmt"
    "github.com/DGHeroin/vault/vfs"
    "github.com/blugelabs/bluge/index"
    segment "github.com/blugelabs/bluge_segment_api"
    "io"
    "io/ioutil"
    "path/filepath"
    "strconv"
)

func NewSearchEngineDir(client *vfs.Client, bucket string) index.Directory {
    dir := &Dir{
        client: client,
        Bucket: bucket,
    }

    return dir
}

type Dir struct {
    client *vfs.Client
    Bucket string
    Prefix string
}

func (s *Dir) Setup(readOnly bool) error {
    return nil
}

func (s *Dir) List(kind string) ([]uint64, error) {
    var itemList []uint64
    val := s.client.ObjectsList(s.Bucket, s.Prefix)

    for obj := range val {
        fileKind := filepath.Ext(obj.Key)
        if fileKind != kind {
            continue
        }

        stringID := filepath.Base(obj.Key)
        stringID = stringID[:len(stringID)-len(kind)]

        parsedID, err := strconv.ParseUint(stringID, 16, 64)
        if err != nil {
            continue
        }

        itemList = append(itemList, parsedID)

    }

    return itemList, nil
}

func (s *Dir) Load(kind string, id uint64) (*segment.Data, io.Closer, error) {
    key := filepath.Join(s.Prefix, s.fileName(kind, id))
    reader, err := s.client.ObjectGet(s.Bucket, key)
    if err != nil {
        return nil, nil, err
    }

    if err != nil {
        return nil, nil, err
    }

    data, err := ioutil.ReadAll(reader)
    if err != nil {
        return nil, nil, err
    }

    return segment.NewDataBytes(data), nil, nil
}

func (s *Dir) Persist(kind string, id uint64, w index.WriterTo, closeCh chan struct{}) error {
    var buf bytes.Buffer
    size, err := w.WriteTo(&buf, closeCh)
    if err != nil {
        return err
    }

    reader := bufio.NewReader(&buf)

    key := filepath.Join(s.Prefix, s.fileName(kind, id))

    _, err = s.client.ObjectPut(s.Bucket, key, reader, size)
    return err
}

func (s *Dir) Remove(kind string, id uint64) error {
    objectToDelete := filepath.Join(s.Prefix, s.fileName(kind, id))

    return s.client.ObjectRemove(s.Bucket, objectToDelete)
}

func (s *Dir) Stats() (numItems uint64, numBytes uint64) {
    objectCount := uint64(0)
    sizeOfObjects := uint64(0)

    objects := s.client.ObjectsList(s.Bucket, "")
    for obj := range objects {
        size := uint64(obj.Size)
        objectCount++
        sizeOfObjects += size
    }

    return objectCount, sizeOfObjects
}

func (s *Dir) Sync() error {
    return nil
}

func (s *Dir) Lock() error {
    return nil
}

func (s *Dir) Unlock() error {
    return nil
}

// ////
func (s *Dir) fileName(kind string, id uint64) string {
    return fmt.Sprintf("%012x", id) + kind
}
