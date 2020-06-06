---
id: instrumentation-events
title: Instrumentation Events
---

Some operations are instrumented using `EventEmitter`. To receive the events use the method `consumer#on`, `producer#on` and `admin#on`, example:

```javascript
const { HEARTBEAT } = consumer.events
const removeListener = consumer.on(HEARTBEAT, e => console.log(`heartbeat at ${e.timestamp}`))

// Remove the listener by invoking removeListener()
```

The listeners are always async, even when using regular functions. The consumer will never block when executing your listeners. Errors in the listeners won't affect the consumer.

Instrumentation Event:

```javascript
{
  id: <Number>,
  type: <String>,
  timestamp: <Number>,
  payload: <Object>
}
```

Events which have `payload.duration` defined, it represent the time between the current event and the last event.


## <a name="list"> List of available events:

### <a name="consumer"></a> Consumer

| order | event               | payload                                                                                                                                                                                                       | description |
|-------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
|1      | REQUEST             | {`broker`, `clientId`, `correlationId`, `size`, `createdAt`, `sentAt`, `pendingDuration`, `duration`, `apiName`, `apiKey`, `apiVersion`} |Whenever a request is made to the broker |
|2      | CONNECT             |                                                                                                                                                                                                               | Consumer connected with broker |
|3      | GROUP_JOIN          | {`groupId`, `memberId`, `leaderId`, `isLeader`, `memberAssignment`, `groupProtocol`, `duration`}                                                                                                                   | Consumer has joined the group |
|4      | FETCH_START         | {}                                                                                                                                                                                                            | Starting to fetch messages from broker |
|5      | FETCH               | {`numberOfBatches`, `duration`}                                                                                                                                                                           | Fetched messages from the broker |
|6      | START_BATCH_PROCESS | {`topic`, `partition`, `highWatermark`, `offsetLag`, `offsetLagLow`, `batchSize`, `firstOffset`, `lastOffset`}                                                                                | Starting to process the batch |
|7      | END_BATCH_PROCESS   | {`topic`, `partition`, `highWatermark`, `offsetLag`, `offsetLagLow`, `batchSize`, `firstOffset`, `lastOffset`, `duration`}                                                                  | Processed the batch |
|8      | COMMIT_OFFSETS      | {`groupId`, `memberId`, `groupGenerationId`, `topics`}                                                                                                                                                | Committed resolved offsets |
|9     | STOP                |                                                                                                                                                                                                               | Consumer has stopped |
|10     | DISCONNECT          |                                                                                                                                                                                                               | Consumer has disconnected |
|-      | CRASH               | {`error`, `groupId`}                                                                                                                                                                                      | Consumer has crashed. After this, consumer will try to perform a full restart if possible, otherwise it will stay hanging. Ideally you should listen to this event and handle yourself. |
|-      | HEARTBEAT           | {`groupId`, `memberId`, `groupGenerationId`}                                                                                                                                                            | Heartbeat sent to the broker |
|-      | REQUEST_TIMEOUT     | {`broker`, `clientId`, `correlationId`, `createdAt`, `sentAt`, `pendingDuration`, `apiName`, `apiKey`, `apiVersion`}                                 | Request to the broker timed out |
|-      | REQUEST_QUEUE_SIZE  | {`broker`, `clientId`, `queueSize`}                                                                                                                                                      | ?? Too many requests? |

### <a name="producer"></a> Producer

* producer.events.CONNECT

* producer.events.DISCONNECT

* producer.events.REQUEST
  payload: {
    `broker`,
    `clientId`,
    `correlationId`,
    `size`,
    `createdAt`,
    `sentAt`,
    `pendingDuration`,
    `duration`,
    `apiName`,
    `apiKey`,
    `apiVersion`
  }

* producer.events.REQUEST_TIMEOUT
  payload: {
    `broker`,
    `clientId`,
    `correlationId`,
    `createdAt`,
    `sentAt`,
    `pendingDuration`,
    `apiName`,
    `apiKey`,
    `apiVersion`
  }

* producer.events.REQUEST_QUEUE_SIZE
  payload: {
    `broker`,
    `clientId`,
    `queueSize`
  }

### <a name="admin"></a> Admin

* admin.events.CONNECT

* admin.events.DISCONNECT

* admin.events.REQUEST
  payload: {
    `broker`,
    `clientId`,
    `correlationId`,
    `size`,
    `createdAt`,
    `sentAt`,
    `pendingDuration`,
    `duration`,
    `apiName`,
    `apiKey`,
    `apiVersion`
  }

* admin.events.REQUEST_TIMEOUT
  payload: {
    `broker`,
    `clientId`,
    `correlationId`,
    `createdAt`,
    `sentAt`,
    `pendingDuration`,
    `apiName`,
    `apiKey`,
    `apiVersion`
  }

* admin.events.REQUEST_QUEUE_SIZE
  payload: {
    `broker`,
    `clientId`,
    `queueSize`
  }
