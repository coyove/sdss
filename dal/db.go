package dal

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/contrib/ngram"
	"github.com/sirupsen/logrus"
	//sync "github.com/sasha-s/go-deadlock"
)

var (
	db       *dynamodb.DynamoDB
	tableFTS = "fts"
)

func InitDB(region, accessKey, secretKey string) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""),
		HTTPClient: &http.Client{
			Timeout: time.Second,
			Transport: &http.Transport{
				MaxConnsPerHost: 200,
			},
		},
	})
	if err != nil {
		panic(err)
	}

	db = dynamodb.New(sess)
	info, err := db.DescribeEndpoints(&dynamodb.DescribeEndpointsInput{})
	if err != nil {
		panic(err)
	}
	logrus.Info("dynamodb endpoint: ", info.Endpoints)
}

func IndexContent(nss []string, id, content string) error {
	// if _, err := db.UpdateItem(&dynamodb.UpdateItemInput{
	// 	TableName: &tableFTS,
	// 	Key: map[string]*dynamodb.AttributeValue{
	// 		"id":  {S: aws.String("d" + id)},
	// 		"id2": {S: aws.String("")},
	// 	},
	// 	UpdateExpression: aws.String("set #a = :value"),
	// 	ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
	// 		":value": {S: aws.String(content)},
	// 	},
	// 	ExpressionAttributeNames: map[string]*string{
	// 		"#a": aws.String("content"),
	// 	},
	// }); err != nil {
	// 	return fmt.Errorf("fts: failed to insert document: %v", err)
	// }

	tokens := ngram.Split(content)
	addDoc(id, content)
	for _, ns := range nss {
		for token := range tokens {
			addBitmap(ns, token, id)
		}
	}
	return nil
}

func SearchContent(ns string, query string) {
	var includes []string
	for k := range ngram.Split(query) {
		includes = append(includes, k)
	}

	var res []string
	mergeBitmaps(ns, includes, nil, clock.UnixMilli(), func(ts int64) error {
		for _, id := range scanDoc(ts) {
			content := getDoc(id)
			score := 0.0
			for _, name := range includes {
				if strings.Contains(content, name) {
					score++
				}
			}
			if score >= float64(len(includes))/2 {
				res = append(res, content)
			}
		}
		if len(res) >= 20 {
			return errMergeAborted
		}
		return nil
	})

	for _, res := range res {
		fmt.Println(res)
	}
}
