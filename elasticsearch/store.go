package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"time"

	"gopkg.in/olivere/elastic.v5"

	"github.com/bingbaba/storage"
)

var (
	HttpClient *http.Client
)

func init() {
	var netTransport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	HttpClient = &http.Client{
		Timeout:   time.Second * 10,
		Transport: netTransport,
	}
}

type store struct {
	client *elastic.Client
}

func NewStore(urls ...string) (*store, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(urls...),
		elastic.SetSniff(false),
		elastic.SetHttpClient(HttpClient),
	)
	if err != nil {
		return nil, err
	}

	return &store{client: client}, nil
}

func (s *store) Create(ctx context.Context, key string, obj interface{}, ttl uint64) error {
	key_array := strings.SplitN(key, "/", 4)
	if len(key_array) != 4 {
		return storage.NewBadRequestError("the key must match \"/index/type/id\" pattern")
	}

	_, err := elastic.NewIndexService(s.client).
		BodyJson(obj).
		Index(key_array[1]).
		Type(key_array[2]).
		Id(key_array[3]).
		Do(ctx)

	if err != nil {
		return storage.NewInternalError(err.Error())
	}

	return nil
}

func (s *store) BulkCreate(ctx context.Context, key string, c chan storage.ChannelObj, ttl uint64) error {
	key_array := strings.SplitN(key, "/", 4)
	if len(key_array) != 4 {
		return storage.NewBadRequestError("the key must match \"/index/type/id\" pattern")
	}

	bp, err := elastic.NewBulkProcessorService(s.client).
		BulkActions(1000).
		FlushInterval(time.Second).Do(ctx)
	if err != nil {
		return storage.NewInternalError(err.Error())
	}
	defer bp.Close()

	for item := range c {
		bp.Add(elastic.NewBulkIndexRequest().
			Index(key_array[1]).
			Type(key_array[2]).
			Id(item.Id).Doc(item.Data))
	}
	bp.Flush()

	return nil
}

func (s *store) Update(ctx context.Context, key string, resourceVersion int64, obj interface{}, ttl uint64) error {
	key_array := strings.SplitN(key, "/", 4)
	if len(key_array) != 4 {
		return storage.NewBadRequestError("the key must match \"/index/type/id\" pattern")
	}

	us := elastic.NewUpdateService(s.client).
		Doc(obj).
		Index(key_array[1]).
		Type(key_array[2]).
		Id(key_array[3])

	if resourceVersion != 0 {
		us = us.Version(resourceVersion)
	}

	_, err := us.Do(ctx)
	if err != nil {
		if elastic.IsNotFound(err) {
			return storage.NewKeyNotFoundError(key, resourceVersion)
		}
		return storage.NewInternalError(err.Error())
	}

	return nil
}

func (s *store) Upsert(ctx context.Context, key string, resourceVersion int64, update_obj, insert_obj interface{}, ttl uint64) error {
	key_array := strings.SplitN(key, "/", 4)
	if len(key_array) != 4 {
		return storage.NewBadRequestError("the key must match \"/index/type/id\" pattern")
	}

	if insert_obj == nil {
		insert_obj = update_obj
	}

	us := elastic.NewUpdateService(s.client).
		Doc(update_obj).
		Upsert(insert_obj).
		Index(key_array[1]).
		Type(key_array[2]).
		Id(key_array[3])

	if resourceVersion != 0 {
		us = us.Version(resourceVersion)
	}

	_, err := us.Do(ctx)
	if err != nil {
		if elastic.IsNotFound(err) {
			return storage.NewKeyNotFoundError(key, resourceVersion)
		}
		return storage.NewInternalError(err.Error())
	}

	return nil
}

func (s *store) Get(ctx context.Context, key string, out interface{}) error {
	key_array := strings.SplitN(key, "/", 4)
	if len(key_array) != 4 {
		return storage.NewBadRequestError("the key must match \"index/type/id\" pattern")
	}
	//for _, key := range key_array {
	//	fmt.Printf("%s\n", key)
	//}

	us := elastic.NewGetService(s.client).
		Index(key_array[1]).
		Type(key_array[2]).
		Id(key_array[3])

	resp, err := us.Do(ctx)
	if err != nil {
		if elastic.IsNotFound(err) {
			return storage.NewKeyNotFoundError(key, 0)
		}
		return storage.NewInternalError(err.Error())
	}

	if out != nil {
		err = json.Unmarshal(*resp.Source, out)
		setResourceVersion(out, *resp.Version)
	}

	return err
}

func (s *store) Delete(ctx context.Context, key string, out interface{}) error {
	key_array := strings.SplitN(key, "/", 4)
	if len(key_array) != 4 {
		return storage.NewBadRequestError("the key must match \"/index/type/id\" pattern")
	}

	us := elastic.NewDeleteService(s.client).
		Index(key_array[1]).
		Type(key_array[2]).
		Id(key_array[3])

	_, err := us.Do(ctx)
	if err != nil {
		if elastic.IsNotFound(err) {
			return storage.NewKeyNotFoundError(key, 0)
		}
		return storage.NewInternalError(err.Error())
	}

	return err
}

func (s *store) DeleteByQuery(ctx context.Context, key string, keyword interface{}) (int64, int64, error) {
	var idx, typ string
	key_array := strings.SplitN(key, "/", 4)
	if len(key_array) < 2 {
		return 0, 0, storage.NewBadRequestError("the key must match \"/index\" pattern")
	}
	idx = key_array[1]
	if len(key_array) >= 3 {
		typ = key_array[2]
	}

	us := elastic.NewDeleteByQueryService(s.client).ProceedOnVersionConflict()
	if idx != "" {
		us = us.Index(idx)
	}
	if typ != "" {
		us = us.Type(typ)
	}

	if keyword != nil {
		query, err := getQueryByKeyword(keyword)
		if err != nil {
			return 0, 0, err
		}
		us = us.Query(query)
	}

	resp, err := us.Do(ctx)
	if err != nil && !elastic.IsConflict(err) {
		return 0, 0, storage.NewInternalError(err.Error())
	}

	return resp.Deleted, resp.VersionConflicts, nil
}

func (s *store) List(ctx context.Context, key string, sp *storage.SelectionPredicate, obj interface{}) ([]interface{}, error) {
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		return nil, storage.NewBadRequestError("non-pointer " + reflect.TypeOf(obj).String())
	}

	key_array := strings.SplitN(key, "/", 4)
	var idx, typ string
	if len(key_array) >= 2 {
		idx = key_array[1]
	}
	if len(key_array) >= 3 {
		typ = key_array[2]
	}

	// index
	var err error
	var resp *elastic.SearchResult
	if sp != nil {
		if sp.ScrollKeepAlive != "" || sp.ScrollId != "" {
			resp, err = s.listByScroll(ctx, idx, typ, sp)
		} else {
			resp, err = s.listBySearch(ctx, idx, typ, sp.Keyword, sp.From, sp.Limit)
		}
	} else {
		resp, err = s.listBySearch(ctx, idx, typ, "", 0, 0)
	}
	if err != nil {
		return nil, err
	}

	return parseSearchResult(resp, obj)
}

func (s *store) listBySearch(ctx context.Context, idx, tpe string, keyword interface{}, from, size int) (resp *elastic.SearchResult, err error) {
	us := elastic.NewSearchService(s.client).Index(idx).Version(true)
	if tpe != "" {
		us = us.Type(tpe)
	}
	//fmt.Printf("%s %s %v %d %d\n", idx, tpe, keyword, from, size)

	// from and size
	if size > 0 {
		us = us.Size(size)
	}
	if from > 0 {
		if from+size > 10000 {
			err = storage.NewBadRequestError("from+size parameter must be less than 10000")
			return
		}
		us = us.From(from)
	}

	if keyword != nil {
		var query elastic.Query
		query, err = getQueryByKeyword(keyword)
		if err != nil {
			return
		}
		if query != nil {
			us = us.Query(query)
		}
	}

	return us.Do(ctx)
}

func (s *store) listByScroll(ctx context.Context, idx, tpe string, sp *storage.SelectionPredicate) (resp *elastic.SearchResult, err error) {
	if sp.EOF == true {
		sp.ScrollId = ""
		return nil, io.EOF
	}

	ss := elastic.NewScrollService(s.client).Index(idx).Version(true)
	if tpe != "" {
		ss = ss.Type(tpe)
	}
	if sp.Limit > 0 {
		ss = ss.Size(sp.Limit)
	}
	if sp.ScrollId != "" {
		ss = ss.ScrollId(sp.ScrollId)
	}

	// query
	if sp.Keyword != nil {
		var query elastic.Query
		query, err = getQueryByKeyword(sp.Keyword)
		if err != nil {
			return
		}
		if query != nil {
			ss = ss.Query(query)
		}
	}

	// source filter
	excludes := ctx.Value("excludes")
	if excludes != nil {
		switch v := excludes.(type) {
		case []string:
			ss = ss.FetchSourceContext(elastic.NewFetchSourceContext(true).Exclude(v...))
		}
	}

	ss = ss.Scroll(sp.ScrollKeepAlive)
	resp, err = ss.Do(ctx)
	if err != nil {
		if err == io.EOF {
			sp.EOF = true
			sp.ScrollId = ""
			return resp, nil
		} else {
			return resp, err
		}
	}
	sp.ScrollId = resp.ScrollId
	return resp, nil
}

func parseSearchResult(resp *elastic.SearchResult, obj interface{}) ([]interface{}, error) {
	if resp == nil || resp.Hits == nil {
		return make([]interface{}, 0), nil
	}

	list := make([]interface{}, len(resp.Hits.Hits))
	for index, hit := range resp.Hits.Hits {
		list[index] = reflect.New(reflect.TypeOf(obj).Elem()).Interface()
		err := json.Unmarshal(*hit.Source, list[index])
		if err != nil {
			return list, err
		}
		//fmt.Printf("%s\n", hit.Source)
		setResourceVersion(list[index], *hit.Version)
	}
	return list, nil
}

func getQueryByKeyword(keyword interface{}) (query elastic.Query, err error) {
	switch v := keyword.(type) {
	case string:
		if v != "" {
			query = elastic.NewQueryStringQuery(keyword.(string))
		}
	case map[string]interface{}:
		querys := make([]elastic.Query, 0, len(v))
		for field, value := range v {
			switch v2 := value.(type) {
			case string, int, int64, float64:
				querys = append(querys, elastic.NewTermQuery(field, v2))
			case []interface{}:
				querys = append(querys, elastic.NewTermsQuery(field, v2...))
			case map[string]interface{}:
				querys = append(querys, NewInterfaceQuery(v2))
			}
		}
		query = elastic.NewBoolQuery().Must(querys...)
	default:
		typ_str := reflect.TypeOf(keyword).Kind().String()
		err = storage.NewBadRequestError("unknown keyword argument: " + typ_str)
	}

	return
}

func setResourceVersion(out interface{}, v int64) {
	switch reflect.TypeOf(out).Kind() {
	case reflect.Ptr:
		v_typ := reflect.TypeOf(out).Elem()
		if v_typ.Kind() == reflect.Map {
			reflect.ValueOf(out).Elem().SetMapIndex(reflect.ValueOf("_version"), reflect.ValueOf(v))
		} else if v_typ.Kind() == reflect.Struct {
			_, ok := reflect.TypeOf(out).Elem().FieldByName("ResourceVersion")
			if ok {
				version_v := reflect.ValueOf(out).Elem().FieldByName("ResourceVersion")
				if version_v.Kind() == reflect.Int64 || version_v.Kind() == reflect.Int {
					version_v.SetInt(v)
				} else if version_v.Kind() == reflect.String {
					version_v.SetString(fmt.Sprintf("%d", v))
				}
			} else {
				//fmt.Printf("not found ResourceVersion\n")
			}
		} else {
			//fmt.Printf("unknown kind: %s\n", v_typ.Kind())
		}
	case reflect.Map:
		reflect.ValueOf(out).SetMapIndex(reflect.ValueOf("_version"), reflect.ValueOf(v))
	default:

	}

}

type InterfaceQuery struct {
	obj map[string]interface{}
}

func NewInterfaceQuery(obj map[string]interface{}) *InterfaceQuery {
	return &InterfaceQuery{obj: obj}
}
func (iq *InterfaceQuery) Source() (interface{}, error) {
	return iq.obj, nil
}
