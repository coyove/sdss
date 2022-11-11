package dal

import (
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

func IndexContent(nss []string, id string, tokens map[string]float64) error {
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

	for _, ns := range nss {
		for token, _ := range tokens {
			AddBitmap(ns+":"+token, id)
		}
	}

	return nil
}
