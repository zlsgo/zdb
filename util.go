package zdb

import (
	"bytes"
	"time"
)

func (j JsonTime) String() string {
	t := time.Time(j)
	if t.IsZero() {
		return "0000-00-00 00:00:00"
	}
	return t.Format("2006-01-02 15:04:05")
}

func (j JsonTime) Time() time.Time {
	return time.Time(j)
}

func (j JsonTime) MarshalJSON() ([]byte, error) {
	res := bytes.NewBufferString("\"")
	res.WriteString(j.String())
	res.WriteString("\"")
	return res.Bytes(), nil
}
