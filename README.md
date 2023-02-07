# ReqDis

ReqDis (short for request distributor) is a go-mod for distributed requests based on Redis pubsub.
The usecase it serves is when a series of service-instances hold distributed data, but an incoming requests needs a comprehensive repsone.
With ReqDis it becomes trivial to send of a request from one instance and collect reponses from (up to) all.

We at vAudience built ReqDis to, for example, respond to a PUBLISH api call to msgbus (<https://msgbus.io>) with the number of connections who actually received the message. Connections being spread over n-instances of our system.

ReqDis allows to not rely on or even require any single-source-of-truth - usually a database - and thereby makes scaling distributed services a lot easier as well.

## VERSIONS

* v0.1.0 initial commit - isolated from a larger project

## INSTALL

````bash
go get -u github.com/itsatony/reqdis
````

## USE

````go

  type TestRequest struct {
    Query string `json:"query"`
  }

  type TestReply struct {
    Value string `json:"value"`
  }

  func Init()
    rc := redis.NewClient(&redis.Options{})
    // nil as *options parameter will lead to default options, which should be fine for most usecases.
    rd := NewReqDis(rc, nil)
    go RequestListener(rd)
    SendRequestViaReqDis()
  }
  
  // this is the basic request handling loop
  // it ranges over the IncomingRequest channel of a ReqDis
  // the PayloadType field of the request Struct is there to allow for simple and safe unmarshaling
  func RequestListener(rd *ReqDis) {
    for request := range rd.IncomingRequest {
      if request.PayloadType == "TestRequest" {
        incomingRequestPayload := TestRequest{}
        err := json.Unmarshal(&incomingRequestPayload)
        if err != nil {
          continue
        }
        // just to demo/simulate latency ... 
        time.Sleep(time.Second)
        replyPayload, err := json.Marshal(TestReply{Value: "testValue"})
        if err != nil {
          continue
        }
        // we send the reply here
        rd.SendNewReply(request.Id, "TestReply", replyPayload)
      }
    }
  }

  func SendRequestViaReqDis() {
    bytePayload, _ := json.Marshal(TestRequest{Query: "testQuery"})
    rdj := rd.SendNewRequest("TestRequest", bytePayload)
    fmt.Printf("[TestJobComplete] req (%s) sent!\n", rdj.Request.Id)
    // using WaitToComplete() is the blocking way to wait for replies. 
    // if you prefer to use a non-blocking approach, 
    // e.g. a go-function in which you have a channel that receives a "complete" notification
    // use the code in WaitToComplete as a reference
    rdj.WaitToComplete()
    fmt.Printf("[TestJobComplete] completed request(%s) with (%d) replies\n", rdj.Request.Id, len(rdj.Replies))
  }

````

## todo

* add example usecase
* turn vars into defaultOptions for NewReqDis
