package se

import (
    "github.com/blugelabs/bluge"
    "time"
)

type (
    Doc struct {
        Id string

        text []struct {
            name  string
            value string
        }
        keyword []struct {
            name  string
            value string
        }
        times []struct {
            name  string
            value time.Time
        }
        number []struct {
            name  string
            value float64
        }
    }
)

func (d *Doc) AddText(field, value string) *Doc {
    d.text = append(d.text, struct {
        name  string
        value string
    }{name: field, value: value})
    return d
}
func (d *Doc) AddKeyword(field, value string) *Doc {
    d.keyword = append(d.keyword, struct {
        name  string
        value string
    }{name: field, value: value})
    return d
}
func (d *Doc) AddTime(field string, t time.Time) *Doc {
    d.times = append(d.times, struct {
        name  string
        value time.Time
    }{name: field, value: t})
    return d
}
func (d *Doc) AddScore(field string, v float64) *Doc {
    d.number = append(d.number, struct {
        name  string
        value float64
    }{name: field, value: v})
    return d
}

func (d *Doc) toDocument() *bluge.Document {
    doc := bluge.NewDocument(d.Id)
    for _, s := range d.text {
        doc.AddField(bluge.NewTextField(s.name, s.value))
    }
    for _, s := range d.keyword {
        doc.AddField(bluge.NewTextField(s.name, s.value))
    }
    for _, s := range d.times {
        doc.AddField(bluge.NewDateTimeField(s.name, s.value))
    }
    for _, s := range d.number {
        doc.AddField(bluge.NewNumericField(s.name, s.value))
    }

    return doc
}
