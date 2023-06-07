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
		Date JsonTime `json:"Date"`
		Name string   `json:"name"`
	}

	// t.Log(scan([]ztype.Map{ztype.Map(m)}, &r))
	t.Log(r)
}
