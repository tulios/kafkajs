---
id: version-1.10.0-instrumentation-events
title: Instrumentation Events
original_id: instrumentation-events
---

> **Experimental** - This feature may be removed or changed in new versions of KafkaJS

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

* consumer.events.HEARTBEAT  
  payload: {`groupId`, `memberId`, `groupGenerationId`}

* consumer.events.COMMIT_OFFSETS  
  payload: {`groupId`, `memberId`, `groupGenerationId`, `topics`}

* consumer.events.GROUP_JOIN  
  payload: {`groupId`, `memberId`, `leaderId`, `isLeader`, `memberAssignment`, `duration`}

* consumer.events.FETCH_START
  payload: {}

* consumer.events.FETCH
  payload: {`numberOfBatches`, `duration`}

* consumer.events.START_BATCH_PROCESS  
  payload: {`topic`, `partition`, `highWatermark`, `offsetLag`, `offsetLagLow`, `batchSize`, `firstOffset`, `lastOffset`}

* consumer.events.END_BATCH_PROCESS  
  payload: {`topic`, `partition`, `highWatermark`, `offsetLag`, `offsetLagLow`, `batchSize`, `firstOffset`, `lastOffset`, `duration`}

* consumer.events.CONNECT

* consumer.events.DISCONNECT

* consumer.events.STOP

* consumer.events.CRASH
  payload: {`error`, `groupId`}

* consumer.events.REQUEST
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

* consumer.events.REQUEST_TIMEOUT
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

* consumer.events.REQUEST_QUEUE_SIZE
  payload: {
    `broker`,
    `clientId`,
    `queueSize`
  }

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
