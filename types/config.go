package types

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
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
