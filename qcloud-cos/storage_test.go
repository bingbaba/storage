package cos

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestCos(t *testing.T) {
	post_map := map[string]string{
		"f1":   "v1",
		"time": fmt.Sprintf("%d", time.Now().Unix()),
	}
	key := "/user/a"

	// CREATE
	s := NewStorage(NewConfigByEnv())
	err := s.Create(
		context.Background(),
		key,
		post_map, 0)
	if err != nil {
		t.Fatal(err)
	}

	// GET
	out := make(map[string]string)
	err = s.Get(
		context.Background(),
		key,
		&out)
	if err != nil {
		t.Fatal(err)
	}

	if out["time"] != post_map["time"] {
		t.Fatalf("expect \"%s\", but get \"%s\"", post_map["time"], out["time"])
	}

	// LIST
	list, err := s.List(context.Background(), "/user", nil, &map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) == 0 {
		t.Fatalf("list /user failed!")
	}
	fmt.Printf("FIRST ITEM IN LIST: %+v\n", list[0])

	// DELETE
	err = s.Delete(context.Background(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
}
