const fs = require('fs')
const ip = require('ip')
const os = require('os')
const path = require('path')
const SnappyCodec = require('kafkajs-snappy')

const { Kafka, logLevel, CompressionTypes, CompressionCodecs } = require('../index')
const sleep = require('../src/utils/sleep')
const { waitFor } = require('../testHelpers')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec
const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  logCreator: PrettyConsoleLogger,
  brokers: [`${host}:9094`, `${host}:9097`, `${host}:9100`],
  clientId: 'example-consumer',
  ssl: {
    servername: 'localhost',
    rejectUnauthorized: false,
    ca: [fs.readFileSync('./testHelpers/certs/cert-signed', 'utf-8')],
  },
  sasl: {
    mechanism: 'plain',
    username: 'test',
    password: 'testtest',
  },
})

const topicName = `topic-test-${Date.now()}`
const producer = kafka.producer()

const messagesToProduce = 1000
const numberOFMessages = 1000

const bigText =
  'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse eu mi sit amet nisl vulputate sagittis ut at justo. Nullam porta eros sit amet nibh interdum fermentum. Aliquam dignissim felis in magna maximus, ut consectetur lacus malesuada. Ut velit lorem, pulvinar vitae elit vel, efficitur sagittis lorem. Nulla risus ex, mollis ut ante et, iaculis elementum turpis. Vivamus vitae ante nisi. Mauris tristique sit amet sapien vitae fermentum. Cras et luctus lacus. Phasellus sit amet quam et turpis tempor rutrum eu at nulla. Maecenas sit amet viverra purus, in posuere nibh. Pellentesque facilisis sit amet mi non volutpat. In interdum urna sit amet rhoncus gravida. Nulla non imperdiet orci. Vestibulum luctus sollicitudin pellentesque. Aliquam porttitor ut quam nec dapibus. Curabitur at dignissim tellus. Nunc blandit, tortor non aliquet ultrices, odio ligula pretium nibh, id faucibus urna eros eget lacus. Curabitur facilisis rutrum tristique. Vestibulum in tristique nunc. Nunc in lorem id nunc sodales viverra id sit amet massa. Aliquam nec suscipit magna. Nam gravida lectus at urna venenatis posuere. Maecenas pharetra neque sed tincidunt molestie. Fusce facilisis maximus eros id sodales. Quisque venenatis purus vel massa tempor fermentum. Phasellus congue pellentesque massa sit amet egestas. Duis neque metus, rutrum a aliquet et, venenatis sit amet ante. Proin sed felis id nibh consectetur mattis. Quisque tempor fermentum nisi non sollicitudin. Fusce posuere faucibus libero et semper. Aliquam ut nibh quis libero aliquet accumsan. Donec nec justo vel eros consequat volutpat in non dolor. Phasellus congue orci et enim porttitor, quis sagittis enim convallis. Praesent est diam, vestibulum eget mauris eleifend, tristique pharetra dui. Nunc imperdiet est non lectus consequat congue. Donec luctus enim mauris, sit amet consequat lectus malesuada non. Nulla vehicula auctor justo non luctus. Sed quis neque sodales, consectetur urna et, venenatis dui. Mauris magna turpis, ullamcorper nec velit convallis, sagittis tristique metus.'

const time = async operation => {
  const start = process.hrtime()
  await operation()
  return process.hrtime(start)
}

const createMessage = num => ({
  key: `key-${num}`,
  value: `value-${num}-${new Date().toISOString()}-${num % 2 === 0 ? 'short' : bigText}`,
  headers: {
    'correlation-id': `${num}-${Date.now()}`,
  },
})

const sendMessage = topic => {
  const messages = Array(numberOFMessages)
    .fill()
    .map((_, i) => createMessage(i))

  return producer
    .send({
      topic,
      compression: CompressionTypes.Snappy,
      messages,
    })
    .catch(e => kafka.logger().error(`[example/producer] ${e.message}`, { stack: e.stack }))
}

const produceMessages = async (numMessages, topic) => {
  for (let i = 0; i < numMessages; i++) {
    await sendMessage(topic)
  }
}

const consumeMessages = async (consumer, expectedNumberOfMessages) => {
  let msgNumber = 0
  await consumer.run({
    eachMessage: async () => {
      msgNumber++
    },
  })

  await waitFor(() => msgNumber >= expectedNumberOfMessages, { delay: 50 })
}

let singleThreadConsumer, multiThreadConsumer, admin

const disconnect = async () => {
  await admin.disconnect()
  await singleThreadConsumer.disconnect()
  await multiThreadConsumer.disconnect()
  await producer.disconnect()
}

const run = async () => {
  admin = kafka.admin()
  await admin.connect()
  await admin.createTopics({
    topics: [
      { topic: `${topicName}-single-thread`, numPartitions: 6 },
      { topic: `${topicName}-multi-thread`, numPartitions: 6 },
    ],
    waitForLeaders: true,
  })

  await admin.disconnect()
  await producer.connect()

  let topic = `${topicName}-single-thread`
  singleThreadConsumer = kafka.consumer({ groupId: 'test-group-single-thread' })
  await singleThreadConsumer.connect()
  await singleThreadConsumer.subscribe({ topic, fromBeginning: true })

  kafka.logger().warn('Starting single thread')
  const singleThreadDuration = await time(async () => {
    await produceMessages(messagesToProduce, topic)
    kafka.logger().info('Messages sent', { topic })
    await consumeMessages(singleThreadConsumer, messagesToProduce * 100)
  })

  await singleThreadConsumer.disconnect()

  kafka.logger().info('Finished single-threaded operations', {
    duration: singleThreadDuration[0] * 1000 + singleThreadDuration[1] / 1000000,
  })

  kafka.startCompressionWorkerPool({
    numberOfThreads: os.cpus().length - 1,
    // logLevel: logLevel.DEBUG,
    setupScriptPath: path.join(__dirname, './workerSetup'),
  })

  await sleep(100)

  topic = `${topicName}-multi-thread`
  multiThreadConsumer = kafka.consumer({ groupId: 'test-group-multi-thread' })
  await multiThreadConsumer.connect()
  await multiThreadConsumer.subscribe({ topic, fromBeginning: true })

  kafka.logger().warn('Starting multi thread')
  const multiThreadDuration = await time(async () => {
    await produceMessages(messagesToProduce, topic)
    kafka.logger().info('Messages sent', { topic })
    await consumeMessages(multiThreadConsumer, messagesToProduce * 100)
  })

  kafka.logger().info('Finished multi-threaded operations', {
    duration: multiThreadDuration[0] * 1000 + multiThreadDuration[1] / 1000000,
  })

  await disconnect()
}

run().catch(e => kafka.logger().error(`[example/consumer] ${e.message}`, { stack: e.stack }))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      kafka.logger().info(`process.on ${type}`)
      kafka.logger().error(e.message, { stack: e.stack })
      await disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    console.log('')
    kafka.logger().info('[example/consumer] disconnecting')
    await disconnect()
  })
})
