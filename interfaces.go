package storage

import (
	"context"
)

type Interface interface {
	Get(ctx context.Context, key string, out interface{}) error
	Create(ctx context.Context, key string, obj interface{}, ttl uint64) error
	BulkCreate(ctx context.Context, key string, c chan ChannelObj, ttl uint64) error
	Delete(ctx context.Context, key string, out interface{}) error
	DeleteByQuery(ctx context.Context, key string, keyword interface{}) (deleted, conflict int64, err error)
	List(ctx context.Context, key string, sp *SelectionPredicate, obj interface{}) ([]interface{}, error)
	Update(ctx context.Context, key string, resourceVersion int64, obj interface{}, ttl uint64) error
	Upsert(ctx context.Context, key string, resourceVersion int64, update_obj, insert_obj interface{}, ttl uint64) error
}

type ChannelObj struct {
	Data interface{}
	Id   string
}
