package types

import (
	"encoding/json"
	"io/ioutil"

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
