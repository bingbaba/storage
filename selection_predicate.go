package storage

type SelectionPredicate struct {
	Keyword interface{}
	Limit   int
	From    int

	ScrollKeepAlive string
	ScrollId        string
	EOF             bool
}
