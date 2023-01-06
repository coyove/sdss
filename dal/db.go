package dal

import (
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/coyove/sdss/contrib/bitmap"
	"github.com/coyove/sdss/types"
	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
	//sync "github.com/sasha-s/go-deadlock"
)

var (
	db        *dynamodb.DynamoDB
	tableFTS  = "fts"
	TagsStore struct {
		*bbolt.DB
		*bitmap.Manager
	}
)

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

	TagsStore.Manager, err = bitmap.NewManager("bitmap_cache/tags", 1024000, 1*1024*1024*1024)
	if err != nil {
		logrus.Fatal("init bitmap manager: ", err)
	}

	TagsStore.DB, err = bbolt.Open("bitmap_cache/tags.db", 0777, &bbolt.Options{FreelistType: bbolt.FreelistMapType})
	if err != nil {
		logrus.Fatal("init tags db: ", err)
	}
}

// func IndexContent(nss []string, doc *types.Document) error {
// 	tokens := ngram.Split(doc.Content)
// 	if err := addDoc(doc); err != nil {
// 		return err
// 	}
//
// 	for _, ns := range nss {
// 		if err := addBitmap(ns, doc.Id, tokens); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }
//
// func SearchContent(ns string, cursor *SearchCursor) (res []*SearchDocument, err error) {
// 	var includes, excludes []string
// 	for k := range ngram.Split(cursor.Query) {
// 		includes = append(includes, k)
// 	}
// 	for k := range ngram.Split(cursor.Exclude) {
// 		excludes = append(excludes, k)
// 	}
//
// 	start, ok := clock.ParseIdStrUnix(cursor.Start)
// 	if !ok {
// 		return nil, fmt.Errorf("invalid cursor start: %q", cursor.Start)
// 	}
//
// 	cursor.Exhausted = true
// 	var docTotal, docHits float64
// 	mergeBitmaps(ns, includes, start, cursor.EndUnix, func(ids []string) bool {
// 		docs, err0 := getDoc(ids)
// 		if err != nil {
// 			err = err0
// 			return false
// 		}
//
// 		for _, doc := range docs {
// 			if doc == nil {
// 				continue
// 			}
//
// 			// fmt.Println("cand", ts, len(docs))
// 			docTotal++
//
// 			if doc.Id > cursor.Start {
// 				continue
// 			}
//
// 			content := doc.Content
// 			score := 0.0
// 			for _, name := range includes {
// 				if strings.Contains(content, name) {
// 					score++
// 				}
// 			}
//
// 			// fmt.Println(ts, doc, score, len(includes), includes)
// 			if score >= float64(len(includes))/2 {
// 				docHits++
// 				res = append(res, &SearchDocument{
// 					Document: *doc,
// 				})
// 			}
//
// 			if len(res) > cursor.Count {
// 				last := res[len(res)-1]
// 				res = res[:len(res)-1]
// 				cursor.Start = last.Id
// 				cursor.Exhausted = false
// 				return false
// 			}
// 		}
// 		return true
// 	})
//
// 	cursor.FalseRate = 0
// 	if docTotal > 0 {
// 		cursor.FalseRate = docHits / docTotal
// 	}
//
// 	for i, res := range res {
// 		fmt.Printf("%02d %s\n", i, res)
// 	}
// 	fmt.Println(cursor.FalseRate, docHits, docTotal, docHits, docTotal)
// 	return
// }
//
// type SearchCursor struct {
// 	Query     string
// 	Exclude   string
// 	Start     string
// 	EndUnix   int64
// 	Count     int
// 	Exhausted bool
// 	FalseRate float64
// }
//
// type SearchDocument struct {
// 	types.Document
// }
