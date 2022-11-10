package dal

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

func IndexContent(ns []string, id string, content string) error {
	if _, err := db.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: &tableFTS,
		Key: map[string]*dynamodb.AttributeValue{
			"id":  {S: aws.String("d" + id)},
			"id2": {S: aws.String("")},
		},
		UpdateExpression: aws.String("set #a = :value"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":value": {S: aws.String(content)},
		},
		ExpressionAttributeNames: map[string]*string{
			"#a": aws.String("content"),
		},
	}); err != nil {
		return fmt.Errorf("fts: failed to insert document: %v", err)
	}

	tokens := ngram.Split(content)
	var errors []error
	for _, ns := range ns {
		var reqs []*dynamodb.WriteRequest
		for token, freq := range tokens {
			tf := strconv.FormatFloat(freq, 'f', 0, 64)
			reqs = append(reqs, &dynamodb.WriteRequest{
				PutRequest: &dynamodb.PutRequest{
					Item: map[string]*dynamodb.AttributeValue{
						"id":      {S: aws.String(ns + ":" + token)},
						"id2":     {S: &id},
						"content": {S: &tf},
					},
				},
			})
		}
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				tableFTS: reqs,
			},
		}

		for i := 0; ; i++ {
			output, err := db.BatchWriteItem(input)
			if err != nil {
				errors = append(errors, fmt.Errorf("fts: [%s/%d] failed to batch write %d tokens: %v", ns, i, len(tokens), err))
				break
			}
			if len(output.UnprocessedItems[tableFTS]) == 0 {
				break
			}
			input.RequestItems = output.UnprocessedItems
			time.Sleep(time.Millisecond * 100)
		}
	}

	if len(errors) > 0 {
		var b []string
		for _, err := range errors {
			b = append(b, err.Error())
		}
		return fmt.Errorf("batch: %v", strings.Join(b, ", "))
	}

	return nil
}
