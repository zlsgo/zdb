package zdb

import (
	"testing"
)

func TestScan(t *testing.T) {
	m := []map[string]interface{}{
		{
			"name": "is name",
			"date": "2021-11-11 15:00:01",
		},
	}
	t.Log(m)
	var r struct {
		Name string   `json:"name"`
		Date JsonTime `json:"Date"`
	}

	// t.Log(scan([]ztype.Map{ztype.Map(m)}, &r))
	t.Log(r)
}
