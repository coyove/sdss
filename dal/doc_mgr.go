package dal

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/types"
)

var cm struct {
	mu sync.Mutex
	m  map[string]*types.Document
}

func addDoc(doc *types.Document) error {
	cm.mu.Lock()
	if cm.m == nil {
		cm.m = map[string]*types.Document{}
	}
	cm.m[doc.Id] = doc
	cm.mu.Unlock()
	// if _, err := db.UpdateItem(&dynamodb.UpdateItemInput{
	// 	TableName: &tableFTS,
	// 	Key: map[string]*dynamodb.AttributeValue{
	// 		"nsid": {S: aws.String("doc")},
	// 		"ts":   {S: aws.String(doc.Id)},
	// 	},
	// 	UpdateExpression: aws.String("set #a = :value"),
	// 	ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
	// 		":value": {B: doc.MarshalBinary()},
	// 	},
	// 	ExpressionAttributeNames: map[string]*string{
	// 		"#a": aws.String("content"),
	// 	},
	// }); err != nil {
	// 	return fmt.Errorf("dal put document: store error: %v", err)
	// }
	return nil
}

func getDoc(id string) (doc *types.Document, err error) {
	// cm.mu.Lock()
	// content = cm.m[id]
	// cm.mu.Unlock()
	resp, err := db.GetItem(&dynamodb.GetItemInput{
		TableName: &tableFTS,
		Key: map[string]*dynamodb.AttributeValue{
			"nsid": {S: aws.String("doc")},
			"ts":   {S: aws.String(id)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dal get document: store error: %v", err)
	}

	v := resp.Item["content"]
	if v == nil {
		return nil, nil
	}
	doc = &types.Document{}
	return doc, json.Unmarshal(v.B, doc)
}

func scanDoc(unix int64) (docs []*types.Document, err error) {
	lower := clock.UnixDeciToIdStr(unix)
	upper := clock.UnixDeciToIdStr(unix + 1)
	for id, doc := range cm.m {
		if id >= lower && id < upper {
			docs = append(docs, doc)
		}
	}
	// run := func(upper string) {
	// 	resp, err := db.Query(&dynamodb.QueryInput{
	// 		TableName:              &tableFTS,
	// 		KeyConditionExpression: aws.String("nsid = :pk and #ts between :lower and :upper"),
	// 		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
	// 			":pk":    {S: aws.String("doc")},
	// 			":lower": {S: aws.String(lower)},
	// 			":upper": {S: aws.String(upper)},
	// 		},
	// 		ExpressionAttributeNames: map[string]*string{
	// 			"#ts": aws.String("ts"),
	// 		},
	// 		ScanIndexForward: aws.Bool(false),
	// 	})
	// 	if err != nil {
	// 		err = fmt.Errorf("dal scan document: store error: %v", err)
	// 		return
	// 	}
	// 	for _, item := range resp.Items {
	// 		doc := &types.Document{}
	// 		json.Unmarshal(item["content"].B, doc)
	// 		docs = append(docs, doc)
	// 	}
	// }
	// run(clock.UnixDeciToIdStr(unix + 1))
	return
}

type int64Heap struct {
	data []int64
}

func (h *int64Heap) Len() int {
	return len(h.data)
}

func (h *int64Heap) Less(i, j int) bool {
	return h.data[i] < h.data[j]
}

func (h *int64Heap) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *int64Heap) Push(x interface{}) {
	h.data = append(h.data, x.(int64))
}

func (h *int64Heap) Pop() interface{} {
	old := h.data
	n := len(old)
	x := old[n-1]
	h.data = old[:n-1]
	return x
}
