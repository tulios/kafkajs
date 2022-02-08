---
id: instrumentation-events
title: Instrumentation Events
---

Some operations are instrumented using `EventEmitter`. To receive the events use the method `consumer.on()`, `producer.on()` and `admin.on()`, example:

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
| REQUEST             | {`broker`, `clientId`, `correlationId`, `size`, `createdAt`, `sentAt`, `pendingDuration`, `duration`, `apiName`, `apiKey`, `apiVersion`} | Emitted on every network request to a broker |
| CONNECT             |                                                                                                                                                                                                               | Consumer connected to a broker. |
| GROUP_JOIN          | {`groupId`, `memberId`, `leaderId`, `isLeader`, `memberAssignment`, `groupProtocol`, `duration`}                                                                                                                   | Consumer has joined the group. |
| FETCH_START         | {}                                                                                                                                                                                                            | Starting to fetch messages from brokers. |
| FETCH               | {`numberOfBatches`, `duration`}                                                                                                                                                                           | Finished fetching messages from the brokers. |
| START_BATCH_PROCESS | {`topic`, `partition`, `highWatermark`, `offsetLag`, `offsetLagLow`, `batchSize`, `firstOffset`, `lastOffset`}                                                                                | Starting user processing of a batch of messages. |
| END_BATCH_PROCESS   | {`topic`, `partition`, `highWatermark`, `offsetLag`, `offsetLagLow`, `batchSize`, `firstOffset`, `lastOffset`, `duration`}                                                                  | Finished processing a batch. This includes user-land processing via `eachMessage`/`eachBatch`. |
| COMMIT_OFFSETS      | {`groupId`, `memberId`, `groupGenerationId`, `topics`}                                                                                                                                                | Committed offsets. |
| STOP                |                                                                                                                                                                                                               | Consumer has stopped. |
| DISCONNECT          |                                                                                                                                                                                                               | Consumer has disconnected. |
| CRASH               | {`error`, `groupId`, `restart`}                                                                                                                                                                                      | Consumer has crashed. In the case of CRASH, the consumer will try to restart itself. If the error is not retriable, the consumer will instead stop and exit. If your application wants to react to the error, such as by cleanly shutting down resources,</br>restarting the consumer itself, or exiting the process entirely, it should listen to the CRASH event. |
| HEARTBEAT           | {`groupId`, `memberId`, `groupGenerationId`}                                                                                                                                                            | Heartbeat sent to the coordinator. |
| REBALANCING         | {`groupId`, `memberId`}                                                                                                                                                            | Consumer Group has started rebalancing. |
| REQUEST_TIMEOUT     | {`broker`, `clientId`, `correlationId`, `createdAt`, `sentAt`, `pendingDuration`, `apiName`, `apiKey`, `apiVersion`}                                 | Request to a broker has timed out. |
| REQUEST_QUEUE_SIZE  | {`broker`, `clientId`, `queueSize`}                                                                                                                                                      | All requests go through a request queue where concurrency is managed (`maxInflightRequests`). Whenever the size of the queue changes, this event is emitted. |
| RECEIVED_UNSUBSCRIBED_TOPICS | {`groupId`, `generationId`, `memberId`, `assignedTopics`, `topicsSubscribed`, `topicsNotSubscribed`} | Event emitted when some members of your consumer group are subscribed to some topics, and some other members of the group are subscribed to a different set of topics. |

### <a name="producer"></a> Producer

| event               | payload                                                                                                                                                                                                       | description |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| REQUEST             | {`broker`, `clientId`, `correlationId`, `size`, `createdAt`, `sentAt`, `pendingDuration`, `duration`, `apiName`, `apiKey`, `apiVersion`} | Emitted on every network request to a broker. |
| CONNECT            |                                                                                                                                                                                                               | Producer connected to a broker. |
| DISCONNECT         |                                                                                                                                                                                                               | Producer has disconnected. |
| REQUEST_TIMEOUT    | {`broker`, `clientId`, `correlationId`, `createdAt`, `sentAt`, `pendingDuration`, `apiName`, `apiKey`, `apiVersion`}                                 | Request to a broker has timed out. |
| REQUEST_QUEUE_SIZE | {`broker`, `clientId`, `queueSize`}                                                                                                                                                      | All requests go through a request queue where concurrency is managed (`maxInflightRequests`). Whenever the size of the queue changes, this event is emitted. |


### <a name="admin"></a> Admin

| event               | payload                                                                                                                                                                                                       | description |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| REQUEST             | {`broker`, `clientId`, `correlationId`, `size`, `createdAt`, `sentAt`, `pendingDuration`, `duration`, `apiName`, `apiKey`, `apiVersion`} | Emitted on every network request to a broker. |
| CONNECT            |                                                                                                                                                                                                               | Admin client connected to a broker |
| DISCONNECT         |                                                                                                                                                                                                               | Admin client has disconnected. |
| REQUEST_TIMEOUT    | {`broker`, `clientId`, `correlationId`, `createdAt`, `sentAt`, `pendingDuration`, `apiName`, `apiKey`, `apiVersion`}                                 | Request to a broker has timed out. |
| REQUEST_QUEUE_SIZE | {`broker`, `clientId`, `queueSize`}                                                                                                                                                      | All requests go through a request queue where concurrency is managed (`maxInflightRequests`). Whenever the size of the queue changes, this event is emitted. |
