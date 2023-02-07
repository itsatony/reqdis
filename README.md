# reqdis
ReqDis (short for request distributor) is a go-mod for distributed requests based on Redis pubsub. The usecase it serves is when a series of service-instances hold distributed data, but an incoming requests needs a comprehensive repsone. With ReqDis it becomes trivial to send of a request from one instance and collect reponses from (up to) all.
