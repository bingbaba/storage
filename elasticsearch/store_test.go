package elasticsearch

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"gopkg.in/olivere/elastic.v5"

	"github.com/bingbaba/storage"
)

func TestStore(t *testing.T) {
	store := newTestStore(t)
	if store == nil {
		return
	}

	testCreate(store, t)

	testGet(store, t)

	testUpdate(store, t)

	testList(store, t)
}

func testCreate(store storage.Interface, t *testing.T) {
	err := store.Create(context.Background(), "/myindex/mytype/myid", newObj(), 0)
	if err != nil {
		t.Fatal(err)
	}
}

func testGet(store storage.Interface, t *testing.T) {
	var obj_map = make(map[string]interface{})
	err := store.Get(context.Background(), "/myindex/mytype/myid", &obj_map)
	if err != nil {
		t.Fatal(err)
	}

	if obj_map["code"].(string) != "myid" {
		t.Fatalf("expect \"myid\", but get \"%s\"", obj_map["code"].(string))
	}
}

func testList(store storage.Interface, t *testing.T) {
	var obj_map = make(map[string]interface{})
	list, err := store.List(context.Background(),
		"/myindex",
		&storage.SelectionPredicate{Keyword: `code:myid`},
		&obj_map)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) == 0 {
		t.Fatal("list failed!")
	}

	var count int64
	sp := &storage.SelectionPredicate{Keyword: `code:myid`, ScrollKeepAlive: "1m", Limit: 1}
	for !sp.EOF {
		list, err := store.List(context.Background(),
			"/btbrrs-customer",
			sp,
			&obj_map)
		if err != nil {
			t.Fatal(err)
		}

		if len(list) == 0 {
			break
		}
		count += int64(len(list))

		//first_map := *(list[0].(*map[string]interface{}))
		//fmt.Printf("scrollid:%s count:%d first: %v\n", sp.ScrollId, count, first_map["code"])
	}

	if count == 0 {
		t.Fatal("scroll list failed!")
	}
}

func testUpdate(store storage.Interface, t *testing.T) {
	err := store.Update(context.Background(),
		"/myindex/mytype/myid",
		0,
		map[string]int{"btbrrsTaskId": 1},
		0,
	)
	if err != nil {
		t.Fatal(err)
	}

	// check
	var obj_map = make(map[string]interface{})
	err = store.Get(context.Background(), "/myindex/mytype/myid", &obj_map)
	if err != nil {
		t.Fatal(err)
	}
}

func newTestStore(t *testing.T) *store {
	urls := strings.Split(os.Getenv("ES_URLS"), ",")
	if len(urls) == 0 {
		return nil
	}

	client, err := elastic.NewClient(
		elastic.SetURL(urls...),
		elastic.SetSniff(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	return &store{client: client}
}

func newObj() interface{} {
	obj_str := `{
    "code": "myid"
}`

	var obj_map map[string]interface{}
	err := json.Unmarshal([]byte(obj_str), &obj_map)
	if err != nil {
		panic(err)
	}

	return obj_map
}
