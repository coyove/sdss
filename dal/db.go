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

func IndexContent(nss []string, id, content string) error {
	tokens := ngram.Split(content)
	addDoc(id, content)

	var out = make(chan error, 1)
	var n = 0
	for _, ns := range nss {
		for token := range tokens {
			doc := &types.DocumentToken{
				Namespace: ns,
				Token:     token,
				Id:        id,
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
	var includes []string
	for k := range ngram.Split(cursor.Query) {
		includes = append(includes, k)
	}

	start, ok := clock.ParseStrUnix(cursor.Start)
	if !ok {
		return nil, fmt.Errorf("invalid cursor start: %q", cursor.Start)
	}

	cursor.Exhausted = true
	mergeBitmaps(ns, includes, nil, start, cursor.EndUnix, func(ts int64) bool {
		for _, id := range scanDoc(ts) {
			if id > cursor.Start {
				continue
			}
			content := getDoc(id)
			score := 0.0
			for _, name := range includes {
				if strings.Contains(content, name) {
					score++
				}
			}
			if score >= float64(len(includes))/2 {
				res = append(res, &SearchDocument{
					Id:      id,
					Content: content,
				})
			}
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

	for i, res := range res {
		fmt.Printf("%02d %s\n", i, res)
	}
	// bm.m.Info(func(k lru.Key, v interface{}, x, y int64) {
	fmt.Println(bm.m.Len())
	// })
	return
}

type SearchCursor struct {
	Query     string
	Start     string
	EndUnix   int64
	Count     int
	Exhausted bool
}

type SearchDocument struct {
	Id      string
	Content string
}

func (doc *SearchDocument) CreateTime() int64 {
	ts, _ := clock.ParseStrUnix(doc.Id)
	return ts
}

func (doc *SearchDocument) String() string {
	return fmt.Sprintf("%d(%s): %q", doc.CreateTime(), doc.Id, doc.Content)
}
