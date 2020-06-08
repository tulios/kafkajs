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

## <a name="list"> List of available events:

### <a name="consumer"></a> Consumer

| event               | payload                                                                                                                                                                                                       | description |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| REQUEST             | {`broker`, `clientId`, `correlationId`, `size`, `createdAt`, `sentAt`, `pendingDuration`, `duration`, `apiName`, `apiKey`, `apiVersion`} |Whenever a request is made to the broker. |
| CONNECT             |                                                                                                                                                                                                               | Consumer connected with broker. |
| GROUP_JOIN          | {`groupId`, `memberId`, `leaderId`, `isLeader`, `memberAssignment`, `groupProtocol`, `duration`}                                                                                                                   | Consumer has joined the group. |
| FETCH_START         | {}                                                                                                                                                                                                            | Starting to fetch messages from broker. |
| FETCH               | {`numberOfBatches`, `duration`}                                                                                                                                                                           | Fetched messages from the broker. |
| START_BATCH_PROCESS | {`topic`, `partition`, `highWatermark`, `offsetLag`, `offsetLagLow`, `batchSize`, `firstOffset`, `lastOffset`}                                                                                | Starting to process the batch. |
| END_BATCH_PROCESS   | {`topic`, `partition`, `highWatermark`, `offsetLag`, `offsetLagLow`, `batchSize`, `firstOffset`, `lastOffset`, `duration`}                                                                  | Processed the batch. |
| COMMIT_OFFSETS      | {`groupId`, `memberId`, `groupGenerationId`, `topics`}                                                                                                                                                | Committed resolved offsets. |
| STOP                |                                                                                                                                                                                                               | Consumer has stopped. |
| DISCONNECT          |                                                                                                                                                                                                               | Consumer has disconnected. |
| CRASH               | {`error`, `groupId`}                                                                                                                                                                                      | Consumer has crashed. </br> In the case of CRASH, the consumer will try to restart itself. If the error is not retriable, the consumer will instead stop and exit. If your application wants to react to the error, such as by cleanly shutting down resources, restarting the consumer itself, or exiting the process entirely, it should listen to the CRASH event. |
| HEARTBEAT           | {`groupId`, `memberId`, `groupGenerationId`}                                                                                                                                                            | Heartbeat sent to the broker. |
| REQUEST_TIMEOUT     | {`broker`, `clientId`, `correlationId`, `createdAt`, `sentAt`, `pendingDuration`, `apiName`, `apiKey`, `apiVersion`}                                 | Request to the broker has timed out. |
| REQUEST_QUEUE_SIZE  | {`broker`, `clientId`, `queueSize`}                                                                                                                                                      | All requests go through a request queue where concurrency is managed (`maxInflightRequests`). Whenever the size of the queue changes, this event is emitted. |

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
