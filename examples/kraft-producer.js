/**
 * KafkaJS Producer Example for Kafka 4.2+ (KRaft mode, no ZooKeeper)
 *
 * Usage:
 *   node examples/kraft-producer.js
 *
 * Environment variables:
 *   KAFKA_BROKERS - comma-separated broker list (default: localhost:9092)
 *   KAFKA_TOPIC   - topic name (default: kraft-test-topic)
 */

const { Kafka, CompressionTypes, logLevel, Partitioners } = require('../index')

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',')
const topic = process.env.KAFKA_TOPIC || 'kraft-test-topic'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers,
  clientId: 'kafkajs-kraft-producer',
})

// Use DefaultPartitioner (Java-compatible) for Kafka 4.2+
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
  // Enable idempotent producer for exactly-once semantics
  idempotent: true,
  maxInFlightRequests: 5,
})

const createMessage = num => ({
  key: `key-${num}`,
  value: JSON.stringify({
    id: num,
    timestamp: new Date().toISOString(),
    message: `Hello from KafkaJS on Kafka 4.2 KRaft!`,
  }),
  headers: {
    'correlation-id': `${num}-${Date.now()}`,
    source: 'kafkajs-kraft-example',
    'kafka-version': '4.2.0',
  },
})

let msgCount = 0

const sendMessages = async () => {
  const count = Math.floor(Math.random() * 10) + 1
  const messages = Array(count)
    .fill()
    .map(() => createMessage(msgCount++))

  kafka.logger().info(`Sending ${messages.length} messages...`)

  try {
    const result = await producer.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages,
    })
    kafka.logger().info('Messages sent successfully', {
      topic,
      partitions: result.map(r => r.partition),
      totalSent: msgCount,
    })
  } catch (error) {
    kafka.logger().error(`Failed to send: ${error.message}`, { stack: error.stack })
  }
}

let intervalId

const run = async () => {
  await producer.connect()
  kafka.logger().info('Producer connected to KRaft cluster', { brokers })

  // Send messages every 3 seconds
  intervalId = setInterval(sendMessages, 3000)

  // Send first batch immediately
  await sendMessages()
}

run().catch(e => {
  kafka.logger().error(`[kraft-producer] ${e.message}`, { stack: e.stack })
  process.exit(1)
})

const shutdown = async () => {
  kafka.logger().info('[kraft-producer] Shutting down...')
  clearInterval(intervalId)
  await producer.disconnect()
  kafka.logger().info(`[kraft-producer] Disconnected. Total messages sent: ${msgCount}`)
}

process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)
