const { Kafka, logLevel } = require('../index')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

const host = process.env.HOST_IP || '127.0.0.1'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  logCreator: PrettyConsoleLogger,
  brokers: [`${host}:9092`],
  clientId: 'example-dlq',
})

const sourceTopic = 'orders'
const dlqTopic = 'orders.dlq'

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'order-processing-group' })

const { deadLetterQueue } = require('../index')

const withDLQ = deadLetterQueue({
  producer,
  topic: dlqTopic,
  maxRetries: 3,
  onOriginalMessageFailed: async ({ topic, partition, message }, error) => {
    kafka.logger().warn('Message sent to DLQ', {
      topic,
      partition,
      offset: message.offset,
      error: error.message,
    })
  },
})

const processOrder = async ({ topic, partition, message }) => {
  const order = JSON.parse(message.value.toString())

  kafka.logger().info('Processing order', {
    topic,
    partition,
    offset: message.offset,
    orderId: order.id,
  })

  // Simulate processing that might fail
  if (!order.id) {
    throw new Error('Order is missing an ID')
  }

  kafka.logger().info('Order processed successfully', { orderId: order.id })
}

const run = async () => {
  await producer.connect()
  await consumer.connect()
  await consumer.subscribe({ topics: [sourceTopic], fromBeginning: true })
  await consumer.run({
    eachMessage: withDLQ(processOrder),
  })
}

run().catch(e => kafka.logger().error(`[example/dlq] ${e.message}`, { stack: e.stack }))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      kafka.logger().info(`process.on ${type}`)
      kafka.logger().error(e.message, { stack: e.stack })
      await consumer.disconnect()
      await producer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    console.log('')
    kafka.logger().info('[example/dlq] disconnecting')
    await consumer.disconnect()
    await producer.disconnect()
  })
})
