import * as fs from 'fs'

import {
  Kafka,
  PartitionAssigners,
  logLevel,
  CompressionTypes,
  CompressionCodecs,
  ResourceTypes,
  LogEntry,
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
  logCreator: (logLevel: logLevel) => (entry: LogEntry) => {},
})

// CONSUMER
const consumer = kafka.consumer({ groupId: 'test-group' })

let removeListener = consumer.on(consumer.events.HEARTBEAT, e =>
  console.log(`heartbeat at ${e.timestamp}`)
)
removeListener()

const runConsumer = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic })
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

removeListener = admin.on(admin.events.CONNECT, e => console.log(`Admin connect at ${e.timestamp}`))
removeListener()

const runAdmin = async () => {
  await admin.connect()
  await admin.fetchTopicMetadata({ topics: ['string'] }).then(metadata => {
    metadata.topics.forEach(topic => {
      console.log(topic.name, topic.partitions)
    })
  })

  await admin.createTopics({
    topics: [{ topic, numPartitions: 10, replicationFactor: 1 }],
    timeout: 30000,
    waitForLeaders: true,
  })

  await admin.describeConfigs({
    includeSynonyms: false,
    resources: [
      {
        type: ResourceTypes.TOPIC,
        name: topic,
      },
    ],
  })

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
