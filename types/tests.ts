import * as fs from 'fs'

import {
  Kafka,
  PartitionAssigners,
  logLevel,
  CompressionTypes,
  CompressionCodecs,
  ConfigResourceTypes,
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
  LogEntry,
  KafkaJSError,
  KafkaJSOffsetOutOfRange,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSConnectionError,
  KafkaJSRequestTimeoutError,
  KafkaJSTopicMetadataNotLoaded,
  KafkaJSStaleTopicMetadataAssignment,
  PartitionMetadata,
  KafkaJSServerDoesNotSupportApiKey,
} from './index'

const { roundRobin } = PartitionAssigners

// COMMON
const host = 'localhost'
const topic = 'topic-test'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${host}:9094`, `${host}:9097`, `${host}:9100`],
  clientId: 'example-consumer',
  ssl: {
    rejectUnauthorized: false,
    ca: [fs.readFileSync('./testHelpers/certs/cert-signed', 'utf-8')],
  },
  sasl: {
    mechanism: 'plain',
    username: 'test',
    password: 'testtest',
  },
  logCreator: (logLevel: logLevel) => (entry: LogEntry) => {
    Date.parse(entry.log.timestamp);
  },
})

kafka.logger().error('Instantiated KafkaJS')

// CONSUMER
const consumer = kafka.consumer({ groupId: 'test-group' })
consumer.logger().info('Instantiated logger', { groupId: 'test-group' })

let removeListener = consumer.on(consumer.events.HEARTBEAT, e =>
  console.log(`heartbeat at ${e.timestamp}`)
)
removeListener()

const runConsumer = async () => {
  await consumer.connect()
  await consumer.subscribe({ topics: [topic] })
  await consumer.run({
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      commitOffsetsIfNecessary,
      uncommittedOffsets,
      isRunning,
      isStale,
    }) => {
      resolveOffset('123')
      await heartbeat()
      commitOffsetsIfNecessary({
        topics: [
          {
            topic: 'topic-name',
            partitions: [{ partition: 0, offset: '500' }],
          },
        ],
      })
      uncommittedOffsets()
      isRunning()
      isStale()
      console.log(batch)
      console.log(batch.offsetLagLow())
    },
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)
    },
  })

  consumer.pause([{ topic: 'topic1' }])
  consumer.pause([{ topic: 'topic2', partitions: [1, 2] }])

  consumer.resume([{ topic: 'topic1' }])
  consumer.resume([{ topic: 'topic1', partitions: [2] }])

  consumer.paused()
  consumer.paused().length
  consumer.paused()[0].topic
  consumer.paused()[0].partitions

  await consumer.commitOffsets([{ topic: 'topic-name', partition: 0, offset: '500' }])
  await consumer.commitOffsets([
    { topic: 'topic-name', partition: 0, offset: '501', metadata: null },
  ])
  await consumer.commitOffsets([
    { topic: 'topic-name', partition: 0, offset: '501', metadata: 'some-metadata' },
  ])
  await consumer.disconnect()
}

runConsumer().catch(console.error)

// PRODUCER
const producer = kafka.producer({ allowAutoTopicCreation: true })
producer.logger().debug('Instantiated producer')

removeListener = producer.on(producer.events.CONNECT, e =>
  console.log(`Producer connect at ${e.timestamp}`)
)
removeListener()

const getRandomNumber = () => Math.round(Math.random() * 1000)
const createMessage = (num: number) => ({
  key: Buffer.from(`key-${num}`),
  value: Buffer.from(`value-${num}-${new Date().toISOString()}`),
})

const sendMessage = () => {
  return producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: Array(getRandomNumber())
        .fill(0)
        .map(_ => createMessage(getRandomNumber())),
    })
    .then(console.log)
    .catch(console.error)
}

const runProducer = async () => {
  await producer.connect()
  setInterval(sendMessage, 3000)
  await producer.disconnect()
}

runProducer().catch(console.error)

// ADMIN
const admin = kafka.admin({ retry: { retries: 10 } })
admin.logger().warn('Instantiated admin')

removeListener = admin.on(admin.events.CONNECT, e => console.log(`Admin connect at ${e.timestamp}`))
removeListener()

const runAdmin = async () => {
  await admin.connect()

  const { controller, brokers, clusterId } = await admin.describeCluster()
  admin.logger().debug('Fetched cluster metadata', {
    controller,
    clusterId,
    brokers: brokers.map(({ nodeId, host, port }) => ({
      nodeId,
      host,
      port,
    })),
  })

  await admin.fetchTopicMetadata({ topics: ['string'] }).then(metadata => {
    metadata.topics.forEach(topic => {
      console.log(topic.name, topic.partitions)
    })
  })

  await admin.listTopics()

  await admin.fetchOffsets({ groupId: 'test-group' })
  await admin.fetchOffsets({ groupId: 'test-group', topics: ['topic1', 'topic2'] })

  await admin.createTopics({
    topics: [{ topic, numPartitions: 10, replicationFactor: 1 }],
    timeout: 30000,
    waitForLeaders: true,
  })

  await admin.describeConfigs({
    includeSynonyms: false,
    resources: [
      {
        type: ConfigResourceTypes.TOPIC,
        name: topic,
      },
    ],
  })

  const describeAcls = await admin.describeAcls({
    resourceName: 'topic-name',
    resourceType: AclResourceTypes.TOPIC,
    host: '*',
    permissionType: AclPermissionTypes.ALLOW,
    operation: AclOperationTypes.ANY,
    resourcePatternType: ResourcePatternTypes.LITERAL,
  })
  admin.logger().debug('Describe ACls', {
    resources: describeAcls.resources.map(r => ({
      resourceType: r.resourceType,
      resourceName: r.resourceName,
      resourcePatternType: r.resourcePatternType,
      acls: r.acls,
    })),
  })

  const createAcls = await admin.createAcls({
    acl: [
      {
        resourceType: AclResourceTypes.TOPIC,
        resourceName: 'topic-name',
        resourcePatternType: ResourcePatternTypes.LITERAL,
        principal: 'User:bob',
        host: '*',
        operation: AclOperationTypes.ALL,
        permissionType: AclPermissionTypes.DENY,
      },
    ],
  })
  admin.logger().debug('Create ACls', {
    created: createAcls === true,
  })

  const deleteAcls = await admin.deleteAcls({
    filters: [
      {
        resourceName: 'topic-name',
        resourceType: AclResourceTypes.TOPIC,
        host: '*',
        permissionType: AclPermissionTypes.ALLOW,
        operation: AclOperationTypes.ANY,
        resourcePatternType: ResourcePatternTypes.LITERAL,
      },
    ],
  })
  admin.logger().debug('Delete ACls', {
    filterResponses: deleteAcls.filterResponses.map(f => ({
      errorCode: f.errorCode,
      errorMessage: f.errorMessage,
      machingAcls: f.matchingAcls.map(m => ({
        errorCode: m.errorCode,
        errorMessage: m.errorMessage,
        resourceType: m.resourceType,
        resourceName: m.resourceName,
        resourcePatternType: m.resourcePatternType,
        principal: m.principal,
        host: m.host,
        operation: m.operation,
        permissionType: m.permissionType,
      })),
    })),
  })

  const { groups } = await admin.listGroups()
  const groupIds = groups.map(({ groupId }) => groupId)
  const groupDescription = await admin.describeGroups(groupIds)
  await admin.deleteGroups(groupDescription.groups.map(({ groupId }) => groupId))

  await admin.disconnect()
}

runAdmin().catch(console.error)

// OTHERS
const produceWithGZIP = async () => {
  await producer.send({
    topic: 'topic-name',
    compression: CompressionTypes.GZIP,
    messages: [{ key: Buffer.from('key1'), value: Buffer.from('hello world!') }],
  })
}

produceWithGZIP().catch(console.error)

const SnappyCodec: any = undefined
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec

kafka.consumer({
  groupId: 'my-group',
  partitionAssigners: [roundRobin],
})

// ERROR
new KafkaJSError('Invalid partition metadata', { retriable: true })
new KafkaJSError('The group is rebalancing')
new KafkaJSError(new Error('💣'), { retriable: true })

new KafkaJSOffsetOutOfRange(new Error(), { topic: topic, partition: 0 })

new KafkaJSNumberOfRetriesExceeded(new Error(), { retryCount: 0, retryTime: 0 })

new KafkaJSConnectionError('Connection error: ECONNREFUSED', {
  broker: `${host}:9094`,
  code: 'ECONNREFUSED',
})
new KafkaJSConnectionError('Connection error: ECONNREFUSED', { code: 'ECONNREFUSED' })

new KafkaJSRequestTimeoutError('Request requestInfo timed out', {
  broker: `${host}:9094`,
  clientId: 'example-consumer',
  correlationId: 0,
  createdAt: 0,
  sentAt: 0,
  pendingDuration: 0,
})

new KafkaJSTopicMetadataNotLoaded('Topic metadata not loaded', { topic: topic })

const partitionMetadata: PartitionMetadata = {
  partitionErrorCode: 0,
  partitionId: 0,
  leader: 2,
  replicas: [2],
  isr: [2],
}
new KafkaJSStaleTopicMetadataAssignment('Topic has been updated', {
  topic: topic,
  unknownPartitions: [partitionMetadata],
})

new KafkaJSServerDoesNotSupportApiKey(
  'The Kafka server does not support the requested API version',
  {
    apiKey: 0,
    apiName: 'Produce',
  }
)
