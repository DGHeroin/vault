package vfs

import (
    "context"
    "github.com/minio/minio-go/v7"
    "github.com/minio/minio-go/v7/pkg/credentials"
    "io"
    "net/url"
    "time"
)

type (
    Client struct {
        client *minio.Client
    }
    Option struct {
        Region    string
        Endpoint  string
        KeyID     string
        KeySecret string
        Secure    bool
    }
    ObjectInfo struct {
        ETag        string   `json:"etag"`
        Key         string   `json:"name"`
        Size        int64    `json:"size"`
        ContentType string   `json:"contentType"`
        MetaData    MetaData `json:"meta"`
    }
    MetaData map[string]string
)

func New(opt Option) (*Client, error) {
    client, err := minio.New(opt.Endpoint, &minio.Options{
        Creds:  credentials.NewStaticV4(opt.KeyID, opt.KeySecret, ""),
        Secure: opt.Secure,
        Region: opt.Region,
    })
    if err != nil {
        return nil, err
    }
    return &Client{client: client}, nil
}
func (c *Client) BucketList() ([]string, error) {
    client := c.client

    var result []string
    b, err := client.ListBuckets(context.Background())
    if err != nil {
        return nil, err
    }
    for _, info := range b {
        result = append(result, info.Name)
    }
    return result, nil
}
func (c *Client) BucketCreate(name string) error {
    client := c.client
    return client.MakeBucket(context.Background(), name, minio.MakeBucketOptions{})
}
func (c *Client) BucketDelete(name string) error {
    client := c.client
    return client.RemoveBucket(context.Background(), name)
}
func (c *Client) ObjectPut(bucket, key string, r io.Reader, objectSize int64, metas ...MetaData) (*ObjectInfo, error) {
    client := c.client

    var (
        userMeta map[string]string
    )
    if len(metas) > 0 {
        userMeta = metas[0]
    }

    resp, err := client.PutObject(context.Background(), bucket, key, r, objectSize, minio.PutObjectOptions{
        SendContentMd5: true,
        UserMetadata:   userMeta,
    })

    if err != nil {
        return nil, err
    }
    info := &ObjectInfo{
        ETag: resp.ETag,
        Key:  resp.Key,
        Size: resp.Size,
    }
    return info, nil
}
func (c *Client) ObjectGet(bucket, key string) (io.Reader, error) {
    client := c.client

    resp, err := client.GetObject(context.Background(), bucket, key, minio.GetObjectOptions{})
    if err != nil {
        return nil, err
    }
    return resp, nil
}
func (c *Client) ObjectRemove(bucket, key string) error {
    client := c.client

    return client.RemoveObject(context.Background(), bucket, key, minio.RemoveObjectOptions{})
}
func (c *Client) ObjectStat(bucket, key string) (*ObjectInfo, error) {
    client := c.client

    info, err := client.StatObject(context.Background(), bucket, key, minio.GetObjectOptions{
        Checksum: true,
    })
    if err != nil {
        return nil, err
    }
    obj := &ObjectInfo{
        ETag:        info.ETag,
        Key:         info.Key,
        Size:        info.Size,
        ContentType: info.ContentType,
        MetaData:    MetaData(info.UserMetadata),
    }
    return obj, nil
}
func (c *Client) ObjectsList(bucket, prefix string) <-chan ObjectInfo {
    client := c.client

    ch := client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{
        Prefix:    prefix,
        Recursive: true,
    })
    csh := make(chan ObjectInfo, 100)
    go func() {
        defer close(csh)
        for info := range ch {
            obj := ObjectInfo{
                ETag:        info.ETag,
                Key:         info.Key,
                Size:        info.Size,
                ContentType: info.ContentType,
            }
            csh <- obj
        }
    }()
    return csh
}
func (c *Client) ObjectPresignedPut(bucket, key string, expires time.Duration) (*url.URL, error) {
    return c.client.PresignedPutObject(context.Background(), bucket, key, expires)
}
func (c *Client) ObjectPresignedGet(bucket, key string, expires time.Duration, reqParams url.Values) (*url.URL, error) {
    return c.client.PresignedGetObject(context.Background(), bucket, key, expires, reqParams)
}
func (c *Client) ObjectPresignedHead(bucket, key string, expires time.Duration, reqParams url.Values) (*url.URL, error) {
    return c.client.PresignedHeadObject(context.Background(), bucket, key, expires, reqParams)
}
