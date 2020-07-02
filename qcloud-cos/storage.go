package cos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/bingbaba/storage"
	"github.com/tencentyun/cos-go-sdk-v5"
)

var (
	asyncLimit = make(chan bool, 50)
)

type store struct {
	*Config
	*cos.Client
}

func NewStorage(conf *Config) *store {
	u, _ := url.Parse(fmt.Sprintf("https://%s-%s.cos.%s.myqcloud.com",
		conf.Bucket,
		conf.AppID,
		conf.Region,
	))
	b := &cos.BaseURL{BucketURL: u}

	return &store{
		Config: conf,
		Client: cos.NewClient(b, &http.Client{
			Timeout: 30 * time.Second,
			Transport: &cos.AuthorizationTransport{
				SecretID:  conf.SecretId,
				SecretKey: conf.SecretKey,
			},
		}),
	}
}

type Config struct {
	AppID     string
	SecretId  string
	SecretKey string
	Bucket    string
	Region    string
}

func NewConfigByEnv() *Config {
	region := os.Getenv("QCLOUD_REGION")
	if region == "" {
		region = "ap-beijing"
	}

	return &Config{
		os.Getenv("QCLOUD_APPID"),
		os.Getenv("QCLOUD_SID"),
		os.Getenv("QCLOUD_SKEY"),
		os.Getenv("QCLOUD_BUCKET"),
		region,
	}
}

func (s *store) Get(ctx context.Context, key string, out interface{}) error {
	opt := &cos.ObjectGetOptions{
		ResponseContentType: "application/octet-stream",
	}
	resp, err := s.Object.Get(context.Background(), parseKey(key), opt)
	if err != nil {
		if strings.Index(err.Error(), "NoSuchKey") >= 0 {
			return storage.NewKeyNotFoundError(key, 0)
		} else {
			return err
		}
	}
	resp.Body.Close()

	if out != nil {
		bs, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		err = json.Unmarshal(bs, out)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *store) Create(ctx context.Context, key string, obj interface{}, ttl uint64) error {
	body, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	opt := &cos.ObjectPutOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			ContentType: "application/octet-stream",
		},
		ACLHeaderOptions: &cos.ACLHeaderOptions{
			//XCosACL: "public-read",
			XCosACL: "private",
		},
	}

	_, err = s.Object.Put(ctx, parseKey(key), bytes.NewReader(body), opt)
	return err
}

func (s *store) BulkCreate(ctx context.Context, key string, c chan storage.ChannelObj, ttl uint64) error {
	for obj := range c {
		s.Create(ctx, key+"/"+obj.Id, obj.Data, 0)
	}

	return nil
}

func (s *store) Delete(ctx context.Context, key string, out interface{}) error {
	_, err := s.Object.Delete(ctx, parseKey(key))
	return err
}

func (s *store) DeleteByQuery(ctx context.Context, key string, keyword interface{}) (deleted, conflict int64, err error) {
	return 0, 0, nil
}

func (s *store) List(ctx context.Context, key string, sp *storage.SelectionPredicate, obj interface{}) ([]interface{}, error) {

	opt := &cos.BucketGetOptions{
		Prefix: parseKey(key),
	}

	if sp != nil {
		if sp.ScrollId != "" {
			opt.Prefix = sp.ScrollId
		}
		if sp.Limit > 0 {
			opt.MaxKeys = sp.Limit
		}
	}

	ret, _, err := s.Client.Bucket.Get(ctx, opt)
	if err != nil {
		return nil, err
	}

	resp := make([]interface{}, len(ret.Contents))
	if sp != nil && sp.KeyOnly {
		for i, c := range ret.Contents {
			resp[i] = c.Key
		}

		return resp, nil
	} else {
		if obj == nil {
			return nil, storage.NewBadRequestError("non-pointer")
		}
		if reflect.TypeOf(obj).Kind() != reflect.Ptr {
			return nil, storage.NewBadRequestError("non-pointer " + reflect.TypeOf(obj).String())
		}

		var wg sync.WaitGroup
		for i, c := range ret.Contents {
			select {
			case <-ctx.Done():
				return resp, ctx.Err()
			case asyncLimit <- true:
				wg.Add(1)
			}

			go func(idx int, c_tmp cos.Object) {
				defer func() {
					wg.Done()
					<-asyncLimit
				}()
				new_obj := reflect.New(reflect.TypeOf(obj).Elem()).Interface()
				err := s.Get(ctx, c_tmp.Key, new_obj)
				if err == nil {
					resp[idx] = new_obj
				}
			}(i, c)
		}
		wg.Wait()
	}
	return resp, nil

}

func (s *store) Update(ctx context.Context, key string, resourceVersion int64, obj interface{}, ttl uint64) error {
	return s.Create(ctx, key, obj, ttl)
}

func (s *store) Upsert(ctx context.Context, key string, resourceVersion int64, update_obj, insert_obj interface{}, ttl uint64) error {
	return s.Create(ctx, key, update_obj, ttl)
}

func parseKey(key string) string {
	return strings.TrimLeft(key, "/")
}
