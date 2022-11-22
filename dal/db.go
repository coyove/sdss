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
	"github.com/coyove/sdss/types"
	"github.com/sirupsen/logrus"
	//sync "github.com/sasha-s/go-deadlock"
)

var (
	db            *dynamodb.DynamoDB
	addBitmapChan = make(chan *types.DocumentToken, 1024)
	tableFTS      = "fts"
)

func init() {
	for i := 0; i < 10; i++ {
		go func() {
			for dt := range addBitmapChan {
				dt.OutError <- addBitmap(dt.Namespace, dt.Token, dt.Id)
			}
		}()
	}
}

func InitDB() {
	ddb := types.Config.DynamoDB
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(ddb.Region),
		Credentials: credentials.NewStaticCredentials(ddb.AccessKey, ddb.SecretKey, ""),
		HTTPClient: &http.Client{
			Timeout: time.Second,
			Transport: &http.Transport{
				MaxConnsPerHost: 200,
			},
		},
	})
	if err != nil {
		logrus.Fatal("init DB: ", err)
	}

	db = dynamodb.New(sess)
	info, err := db.DescribeEndpoints(&dynamodb.DescribeEndpointsInput{})
	if err != nil {
		logrus.Fatal("init DB, describe: ", err)
	}
	for _, ep := range info.Endpoints {
		logrus.Info("dynamodb endpoint: ", strings.Replace(ep.String(), "\n", " ", -1))
	}
}

func IndexContent(nss []string, doc *types.Document) error {
	tokens := ngram.Split(doc.Content)
	if err := addDoc(doc); err != nil {
		return err
	}

	var out = make(chan error, 1)
	var n = 0
	for _, ns := range nss {
		for token := range tokens {
			doc := &types.DocumentToken{
				Namespace: ns,
				Token:     token,
				Id:        doc.Id,
				OutError:  out,
			}
			addBitmapChan <- doc
			n++
		}
	}
	var lastErr error
	for i := 0; i < n; i++ {
		if err := <-out; err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func SearchContent(ns string, cursor *SearchCursor) (res []*SearchDocument, err error) {
	var includes, excludes []string
	for k := range ngram.Split(cursor.Query) {
		includes = append(includes, k)
	}
	for k := range ngram.Split(cursor.Exclude) {
		excludes = append(excludes, k)
	}

	start, ok := clock.ParseStrUnixDeci(cursor.Start)
	if !ok {
		return nil, fmt.Errorf("invalid cursor start: %q", cursor.Start)
	}

	cursor.Exhausted = true
	var tsTotal, tsHits, docTotal, docHits float64
	mergeBitmaps(ns, includes, excludes, start, cursor.EndUnix, func(ts int64) bool {
		docs, err0 := scanDoc(ts)
		if err != nil {
			err = err0
			return false
		}

		// fmt.Println("cand", ts, len(docs))
		docTotal += float64(len(docs))
		tsTotal += 1

		found := false
		for _, doc := range docs {
			if doc.Id > cursor.Start {
				continue
			}
			content := doc.Content
			score := 0.0
			for _, name := range includes {
				if strings.Contains(content, name) {
					score++
				}
			}

			// fmt.Println(ts, doc, score, len(includes), includes)
			if score >= float64(len(includes))/2 {
				docHits++
				res = append(res, &SearchDocument{
					Document: *doc,
				})
				found = true
			}
		}

		if found {
			tsHits++
		}

		if len(res) > cursor.Count {
			last := res[len(res)-1]
			res = res[:len(res)-1]
			cursor.Start = last.Id
			cursor.Exhausted = false
			return false
		}
		return true
	})

	cursor.FalseRate = 0
	if tsTotal > 0 {
		cursor.FalseRate = tsHits / tsTotal
	}

	for i, res := range res {
		fmt.Printf("%02d %s\n", i, res)
	}
	fmt.Println(cursor.FalseRate, tsHits, tsTotal, docHits, docTotal)
	return
}

type SearchCursor struct {
	Query     string
	Exclude   string
	Start     string
	EndUnix   int64
	Count     int
	Exhausted bool
	FalseRate float64
}

type SearchDocument struct {
	types.Document
}
