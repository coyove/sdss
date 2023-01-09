package types

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

var Config struct {
	DynamoDB struct {
		Region    string
		AccessKey string
		SecretKey string
	}
}

func LoadConfig(path string) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		logrus.Fatal("load config: ", err)
	}
	if err := json.Unmarshal(buf, &Config); err != nil {
		logrus.Fatal("load config unmarshal: ", err)
	}
}

type Request struct {
	*http.Request
	Start time.Time
	T     map[string]interface{}

	paging struct {
		built    bool
		current  int
		desc     bool
		pageSize int
		sort     int
	}
}

func (r *Request) AddTemplateValue(k string, v interface{}) {
	if r.T == nil {
		r.T = map[string]interface{}{}
	}
	r.T[k] = v
}

func (r *Request) Elapsed() int64 {
	return int64(time.Since(r.Start).Milliseconds())
}

func (r *Request) GetPagingArgs() (int, int, bool, int) {
	if r.paging.built {
		return r.paging.current, r.paging.sort, r.paging.desc, r.paging.pageSize
	}

	p, _ := strconv.Atoi(r.URL.Query().Get("p"))
	if p < 1 {
		p = 1
	}
	sort, _ := strconv.Atoi(r.URL.Query().Get("sort"))
	if sort < -1 || sort > 1 {
		sort = 0
	}
	desc := r.URL.Query().Get("desc") == "1"
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("pagesize"))
	if pageSize <= 0 {
		pageSize = 50
	}
	if pageSize > 50 {
		pageSize = 50
	}

	r.paging.current, r.paging.sort, r.paging.desc, r.paging.pageSize = p, sort, desc, pageSize
	r.paging.built = true
	return p, sort, desc, pageSize
}

func (r *Request) BuildPageLink(p int) string {
	_, sort, desc, pageSize := r.GetPagingArgs()
	d := "1"
	if !desc {
		d = ""
	}
	return fmt.Sprintf("p=%d&sort=%d&desc=%v&pagesize=%d", p, sort, d, pageSize)
}
