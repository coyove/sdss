package types

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

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
	T map[string]interface{}
}

func (r *Request) AddTemplateValue(k string, v interface{}) {
	if r.T == nil {
		r.T = map[string]interface{}{}
	}
	r.T[k] = v
}
