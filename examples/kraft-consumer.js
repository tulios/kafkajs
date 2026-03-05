/**
 * KafkaJS Consumer Example for Kafka 4.2+ (KRaft mode, no ZooKeeper)
 *
 * Usage:
 *   node examples/kraft-consumer.js
 *
 * Environment variables:
 *   KAFKA_BROKERS  - comma-separated broker list (default: localhost:9092)
 *   KAFKA_TOPIC    - topic name (default: kraft-test-topic)
 *   KAFKA_GROUP_ID - consumer group ID (default: kraft-test-group)
 */

const { Kafka, logLevel } = require('../index')

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',')
const topic = process.env.KAFKA_TOPIC || 'kraft-test-topic'
const groupId = process.env.KAFKA_GROUP_ID || 'kraft-test-group'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers,
  clientId: 'kafkajs-kraft-consumer',
})

const consumer = kafka.consumer({
  groupId,
  // Rack-aware consumption (KIP-1227 - rack ID exposed in Kafka 4.2)
  rackId: process.env.KAFKA_RACK_ID || '',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
})

let messageCount = 0

const run = async () => {
  await consumer.connect()
  kafka.logger().info('Consumer connected to KRaft cluster', { brokers, groupId })

  await consumer.subscribe({ topics: [topic], fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      messageCount++

      const headers = Object.keys(message.headers || {}).reduce(
        (acc, key) => ({
          ...acc,
          [key]: message.headers[key].toString(),
        }),
        {}
      )

      kafka.logger().info('Message received', {
        topic,
        partition,
        offset: message.offset,
        timestamp: message.timestamp,
        key: message.key ? message.key.toString() : null,
        value: message.value.toString(),
        headers,
        messageCount,
      })
    },
  })
}

run().catch(e => {
  kafka.logger().error(`[kraft-consumer] ${e.message}`, { stack: e.stack })
  process.exit(1)
})

const shutdown = async () => {
  kafka.logger().info('[kraft-consumer] Shutting down...')
  await consumer.disconnect()
  kafka.logger().info(`[kraft-consumer] Disconnected. Total messages consumed: ${messageCount}`)
}

process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)
