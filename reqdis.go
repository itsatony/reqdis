package reqdis

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/lithammer/shortuuid/v3"
)

// @title reqdis
// @version 0.2.0
// @description ReqDis (short for request distributor) is a go-mod for distributed requests based on Redis pubsub
// @contact.name Toni
// @contact.email i+reqdis@itsaatony.com
// @license.name Unlicense
// @license.url http://unlicense.org/

// ========================= ReqDis Options

type ReqDisOptions struct {
	BufferSize             int
	InstanceAliveInterval  time.Duration
	InstancePruneInterval  time.Duration
	RequestTTL             time.Duration
	Redis_KeyPrefix        string
	Redis_KeyPartSeparator string
	Ctx                    context.Context
}

func NewReqDisOptions() ReqDisOptions {
	return ReqDisOptions{
		BufferSize:             10,
		InstanceAliveInterval:  time.Duration(time.Second * 2),
		InstancePruneInterval:  time.Duration(time.Second * 4),
		RequestTTL:             time.Duration(time.Second * 5),
		Redis_KeyPrefix:        "reqdis",
		Redis_KeyPartSeparator: ":",
		Ctx:                    context.Background(),
	}
}

// ========================= ReqDis

func NewReqDis(redisClient redis.UniversalClient, options *ReqDisOptions) *ReqDis {
	var rd ReqDis = ReqDis{}
	if options == nil {
		opts := NewReqDisOptions()
		options = &opts
	}
	rd.Options = options
	rd.InstanceId = shortuuid.New()
	rd.redisClient = redisClient
	rd.AliveInstances = sync.Map{} // make(map[string]time.Time)
	rd.ActiveJobs = sync.Map{}     //make(map[string]*ReqDisJob)
	rd.IncomingRequest = make(chan *ReqDisMessage, options.BufferSize)
	// rd.CompletedJob = make(chan ReqDisJob, BufferSize)
	rd.pruneInstancesTicker = time.NewTicker(options.InstancePruneInterval)
	go rd.pruneTicks()
	rd.sendAliveTicker = time.NewTicker(options.InstanceAliveInterval)
	go rd.aliveTicks()
	go rd.subscribe()
	rd.SendAlive()
	return &rd
}

type ReqDis struct {
	Options         *ReqDisOptions
	InstanceId      string
	redisClient     redis.UniversalClient
	subscriber      *redis.PubSub
	IncomingRequest chan *ReqDisMessage
	// Safety               sync.Mutex
	ActiveJobs           sync.Map //map[string]*ReqDisJob
	AliveInstances       sync.Map //map[string]time.Time
	pruneInstancesTicker *time.Ticker
	sendAliveTicker      *time.Ticker
}

func (rd *ReqDis) MsgType_Keepalive() string {
	return "keepalive"
}
func (rd *ReqDis) MsgType_Request() string {
	return "request"
}
func (rd *ReqDis) MsgType_Reply() string {
	return "reply"
}
func (rd *ReqDis) KnownInstanceCount() int {
	var n int = 0
	rd.AliveInstances.Range(func(key any, value any) bool {
		n += 1
		return true
	})
	return n
}

func (rd *ReqDis) pruneTicks() {
	for {
		<-rd.pruneInstancesTicker.C
		rd.PruneOldInstances()
	}
}

func (rd *ReqDis) aliveTicks() {
	for {
		<-rd.sendAliveTicker.C
		rd.SendAlive()
	}
}

func (rd *ReqDis) getSubscribeTopic() (topic string) {
	topicParts := []string{
		rd.Options.Redis_KeyPrefix,
		"*",
	}
	topic = strings.Join(topicParts, rd.Options.Redis_KeyPartSeparator)
	return topic
}

func (rd *ReqDis) PruneOldInstances() {
	IdsToRemove := []string{}
	now := time.Now()

	rd.AliveInstances.Range(func(key any, value any) bool {
		id, ok := key.(string)
		if !ok {
			return true
		}
		lastAlive, ok := value.(time.Time)
		if !ok {
			return true
		}
		if lastAlive.Before(now.Add(-rd.Options.InstancePruneInterval)) {
			IdsToRemove = append(IdsToRemove, id)
		}
		return true
	})
	for _, id := range IdsToRemove {
		rd.AliveInstances.LoadAndDelete(id)
	}
}

func (rd *ReqDis) AddJob(job *ReqDisJob) *ReqDis {
	rd.ActiveJobs.Store(job.Request.Id, job)
	return rd
}

func (rd *ReqDis) RemoveJob(requestId string) *ReqDis {
	rd.ActiveJobs.LoadAndDelete(requestId)
	return rd
}

func (rd *ReqDis) CreateTopic(rdm *ReqDisMessage) (topic string) {
	topicParts := []string{
		rd.Options.Redis_KeyPrefix, rdm.MessageType, rd.InstanceId, rdm.Id,
	}
	topic = strings.Join(topicParts, rd.Options.Redis_KeyPartSeparator)
	return topic
}

func (rd *ReqDis) SendAlive() {
	rdm := NewReqDisMessage(rd.InstanceId, rd.MsgType_Keepalive(), 0)
	rd.redisClient.Publish(rd.Options.Ctx, rd.CreateTopic(rdm), rdm.ToJson()).Err()
}

func (rd *ReqDis) SendNewRequest(payloadType string, payload []byte) *ReqDisJob {
	// we expect a reply own instance (1) + from every instance in our list!
	count := rd.KnownInstanceCount()
	mtype := rd.MsgType_Request()
	rdm := NewReqDisMessage(rd.InstanceId, mtype, count)
	rdm.SetPayload(payloadType, payload)
	rdj := NewReqDisJob(rdm)
	rd.AddJob(rdj)
	rdj.Send(rd, rd.Options.RequestTTL)
	return rdj
}

func (rd *ReqDis) SendNewReply(RequestId string, payloadType string, payload []byte) *ReqDisMessage {
	rdm := NewReqDisMessage(rd.InstanceId, rd.MsgType_Reply(), 0)
	rdm.ReplyToRequestId = RequestId
	rdm.SetPayload(payloadType, payload)
	rd.redisClient.Publish(rd.Options.Ctx, rd.CreateTopic(rdm), rdm.ToJson()).Err()
	return rdm
}

func (rd *ReqDis) handleReply(incomingMsg *ReqDisMessage) {
	rd.ActiveJobs.Range(func(key any, value any) bool {
		job, ok := value.(*ReqDisJob)
		if !ok {
			return true
		}
		if job.Request.Id == incomingMsg.ReplyToRequestId {
			job.HandleReply(incomingMsg)
		}
		return true
	})
}

func (rd *ReqDis) handleRequest(incomingMsg *ReqDisMessage) {
	rd.IncomingRequest <- incomingMsg
}

func (rd *ReqDis) handleKeepAlive(incomingMsg *ReqDisMessage) {
	rd.AliveInstances.Store(incomingMsg.InstanceId, time.Now())
}

func (rd *ReqDis) subscribe() {
	rd.subscriber = rd.redisClient.PSubscribe(rd.Options.Ctx, rd.getSubscribeTopic())
	incomingPubsChannel := rd.subscriber.Channel()
	for msg := range incomingPubsChannel {
		IncomingEntity := ReqDisMessage{}
		err := json.Unmarshal([]byte(msg.Payload), &IncomingEntity)
		if err != nil {
			continue
		}
		if IncomingEntity.MessageType == rd.MsgType_Keepalive() {
			rd.handleKeepAlive(&IncomingEntity)
		} else if IncomingEntity.MessageType == rd.MsgType_Request() {
			rd.handleRequest(&IncomingEntity)
		} else if IncomingEntity.MessageType == rd.MsgType_Reply() {
			rd.handleReply(&IncomingEntity)
		}
	}
}

// ========================= ReqDisJob
type ReqDisJob struct {
	SentTimestamp     time.Time `json:"sentTimestamp"`
	CompleteTimestamp time.Time `json:"completeTimestamp"`
	TimeToLive        time.Duration
	State             string
	ForceEndTicker    *time.Ticker
	Request           *ReqDisMessage
	Safety            sync.Mutex
	Replies           map[string]*ReqDisMessage
	StateSignal       chan string
}

func NewReqDisJob(rdm *ReqDisMessage) *ReqDisJob {
	rdj := ReqDisJob{
		Request:     rdm,
		StateSignal: make(chan string, 10),
		Replies:     make(map[string]*ReqDisMessage),
	}
	rdj.SetState(rdj.State_Active())
	return &rdj
}

func (rdj *ReqDisJob) Send(rd *ReqDis, ttl time.Duration) {
	rdj.SentTimestamp = time.Now()
	rdj.TimeToLive = ttl
	rdj.ForceEndTicker = time.NewTicker(rdj.TimeToLive)
	rd.redisClient.Publish(rd.Options.Ctx, rd.CreateTopic(rdj.Request), rdj.Request.ToJson()).Err()
	go rdj.ticks()
}

func (rdj *ReqDisJob) ticks() {
	for {
		<-rdj.ForceEndTicker.C
		rdj.Complete()
	}
}

func (rdj *ReqDisJob) HandleReply(rdm *ReqDisMessage) {
	// nuts.L.Debugf("[HandleReply] rdmId(%s)!", rdm.Id)
	if rdj.IsComplete() {
		return
	}
	rdj.Safety.Lock()
	defer rdj.Safety.Unlock()
	rdj.Replies[rdm.Id] = rdm
	count := len(rdj.Replies)
	// nuts.L.Debugf("[HandleReply] replyCount = (%d) of expected(%d)!", count, rdj.Request.ExpectedReplies)
	if count >= rdj.Request.ExpectedReplies {
		rdj.Complete()
	}
}

func (rdj *ReqDisJob) WaitToComplete() {
	rdy := rdj.State_Complete()
	for state := range rdj.StateSignal {
		if state == rdy {
			break
		}
	}
}

func (rdj *ReqDisJob) IsComplete() bool {
	return rdj.State == rdj.State_Complete()
}

func (rdj *ReqDisJob) Complete() {
	// nuts.L.Debugf("[Complete] rdmId(%s)!", rdj.Request.Id)
	rdj.ForceEndTicker.Stop()
	rdj.SetState(rdj.State_Complete())
}

func (rdj *ReqDisJob) State_Active() string {
	return "active"
}

func (rdj *ReqDisJob) State_Complete() string {
	return "complete"
}

func (rdj *ReqDisJob) SetState(state string) {
	rdj.State = state
	rdj.sendState(rdj.State)
}

func (rdj *ReqDisJob) sendState(state string) {
	rdj.StateSignal <- state
}

// ========================= ReqDisMessage

func NewReqDisMessage(instanceId string, msgType string, expRep int) *ReqDisMessage {
	rdm := ReqDisMessage{}
	rdm.InstanceId = instanceId
	rdm.Id = shortuuid.New()
	rdm.MessageType = msgType
	rdm.ExpectedReplies = expRep
	return &rdm
}

type ReqDisMessage struct {
	Id               string `json:"id"`
	InstanceId       string `json:"instanceId"`
	MessageType      string `json:"msgType"`          // any of ReqDis.MsgType_Reply() || ReqDis.MsgType_Request() || ReqDis.MsgType_Keepalive()
	ReplyToRequestId string `json:"replyToRequestId"` // only needed/used when MessageType is "reply"
	PayloadType      string `json:"payloadType"`
	Payload          []byte `json:"payload"`
	ExpectedReplies  int    `json:"expectedReplies"`
}

func (rdm *ReqDisMessage) ToJson() (jsonString string) {
	jsonBytes, err := json.Marshal(rdm)
	if err != nil {
		return ""
	}
	return string(jsonBytes)
}

func (rdm *ReqDisMessage) SetPayload(payloadType string, payload []byte) (reqDisRequest *ReqDisMessage) {
	rdm.PayloadType = payloadType
	rdm.Payload = payload
	return rdm
}
