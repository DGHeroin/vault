package se

import (
    "context"
    "github.com/blugelabs/bluge"
    "github.com/blugelabs/bluge/index"
    "time"
)

type (
    Indexing struct {
        w *bluge.Writer
    }
)

func New(path string, dir ...index.Directory) (*Indexing, error) {
    i := &Indexing{}

    config := bluge.DefaultConfigWithDirectory(func() index.Directory {
        if len(dir) > 0 && dir[0] != nil {
            return dir[0]
        }
        return index.NewFileSystemDirectory(path)
    })

    w, err := bluge.OpenWriter(config)
    if err != nil {
        return nil, err
    }
    i.w = w
    return i, nil
}

func (i *Indexing) Close() error {
    return i.w.Close()
}
func (i *Indexing) Add(doc *Doc) error {
    return i.AddBatch(doc)
}
func (i *Indexing) AddBatch(docs ...*Doc) error {
    bat := bluge.NewBatch()
    for _, doc := range docs {
        d := doc.toDocument()
        bat.Update(d.ID(), d)
    }
    return i.w.Batch(bat)
}

func (i *Indexing) Search(N int, field string, keyword string, fn func(id string) bool) {
    r, err := i.w.Reader()
    if err != nil {
        return
    }
    defer func() {
        recover()
        _ = r.Close()
    }()

    query := bluge.NewMatchQuery(keyword).SetField(field)
    executeTopNQuery(r, N, query, fn)
}
func (i *Indexing) SearchFuzz(N int, field string, keyword string, fn func(id string) bool) {
    r, err := i.w.Reader()
    if err != nil {
        return
    }
    defer func() {
        recover()
        _ = r.Close()
    }()

    query := bluge.NewFuzzyQuery(keyword).SetField(field)
    executeTopNQuery(r, N, query, fn)
}

func (i *Indexing) SearchTimeRange(N int, filed string, t0, t1 time.Time, fn func(id string) bool) {
    r, err := i.w.Reader()
    if err != nil {
        return
    }
    defer func() {
        recover()
        _ = r.Close()
    }()

    query := bluge.NewDateRangeQuery(t0, t1).SetField(filed)
    executeTopNQuery(r, N, query, fn)
}
func (i *Indexing) SearchNumberRange(N int, filed string, v0, v1 float64, fn func(id string) bool) {
    r, err := i.w.Reader()
    if err != nil {
        return
    }
    defer func() {
        recover()
        _ = r.Close()
    }()

    query := bluge.NewNumericRangeInclusiveQuery(v0, v1, true, true).SetField(filed)
    executeTopNQuery(r, N, query, fn)
}
func executeTopNQuery(r *bluge.Reader, N int, query bluge.Query, fn func(string) bool) {
    request := bluge.NewTopNSearch(N, query).WithStandardAggregations()
    it, err := r.Search(context.Background(), request)
    if err != nil {
        return
    }
    for {
        m, err := it.Next()
        if err != nil {
            break
        }
        if m == nil {
            break
        }

        err = m.VisitStoredFields(func(field string, value []byte) bool {
            if field == "_id" {
                return fn(string(value))
            }
            return true
        })
        if err != nil {
            break
        }
    }
}
