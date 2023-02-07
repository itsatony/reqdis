package reqdis

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	nuts "github.com/vaudience/go-nuts"
)

var RDs []*ReqDis

func InitRDS(n int) {
	for i := 1; i < n; i++ {
		options := redis.UniversalOptions{}
		// redisClient = redis.NewClient(&options)
		rc := redis.NewUniversalClient(&options)
		rd := NewReqDis(rc, nil)
		go RequestListener(rd)
		RDs = append(RDs, rd)
	}
	nuts.L.Debugf("[InitRDS] waiting for initial KeepAlive messages to arrive")
	time.Sleep(RDs[0].Options.InstancePruneInterval)
	nuts.L.Debugf("[InitRDS] ready")
}

type TestRequest struct {
	Query string `json:"query"`
}
type TestReply struct {
	Value string `json:"value"`
}

func RequestListener(rd *ReqDis) {
	for request := range rd.IncomingRequest {
		if request.PayloadType == "TestRequest" {
			// nuts.L.Debugf("[RequestListener.%s] received new TestRequest(%s)", rd.InstanceId, request.Id)
			payload := TestReply{Value: "testValue"}
			bytePayload, _ := json.Marshal(payload)
			time.Sleep(time.Second)
			rd.SendNewReply(request.Id, "TestReply", bytePayload)
		}
	}
}

func TestJobComplete(t *testing.T) {
	InitRDS(6)
	payload := TestRequest{Query: "testQuery"}
	bytePayload, _ := json.Marshal(payload)
	rdj := RDs[0].SendNewRequest("TestRequest", bytePayload)
	nuts.L.Debugf("[TestJobComplete] req (%s) sent!", rdj.Request.Id)
	rdj.WaitToComplete()
	nuts.L.Debugf("[TestJobComplete] completed request(%s) with (%d) replies", rdj.Request.Id, len(rdj.Replies))
	rdj2 := RDs[1].SendNewRequest("TestRequest", bytePayload)
	nuts.L.Debugf("[TestJobComplete] req (%s) sent!", rdj.Request.Id)
	rdj2.WaitToComplete()
	nuts.L.Debugf("[TestJobComplete] completed request(%s) with (%d) replies", rdj2.Request.Id, len(rdj2.Replies))
}
