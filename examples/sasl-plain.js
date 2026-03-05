/**
 * Example: SASL PLAIN Authentication
 *
 * The simplest SASL mechanism. Sends username and password in cleartext.
 * IMPORTANT: Always use with SSL/TLS to avoid sending credentials in plain text.
 *
 * Broker configuration (server.properties):
 *   sasl.enabled.mechanisms=PLAIN
 *   listener.security.protocol.map=SASL_SSL:SASL_SSL
 *
 * Usage:
 *   SSL=true \
 *   SASL_USERNAME=my-user \
 *   SASL_PASSWORD=my-password \
 *   KAFKA_BROKERS=kafka1:9092,kafka2:9092 \
 *   node examples/sasl-plain.js
 */

const { Kafka, logLevel } = require('../index')

const kafka = new Kafka({
  clientId: 'sasl-plain-example',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  logLevel: logLevel.INFO,

  // Always use SSL with PLAIN to encrypt credentials in transit
  ssl: process.env.SSL === 'true' || undefined,

  sasl: {
    mechanism: 'plain',
    username: process.env.SASL_USERNAME || 'test',
    password: process.env.SASL_PASSWORD || 'testtest',
  },
})

const run = async () => {
  const admin = kafka.admin()
  await admin.connect()
  console.log('Connected with SASL PLAIN authentication')

  const topics = await admin.listTopics()
  console.log('Topics:', topics)

  await admin.disconnect()
}

run().catch(e => {
  console.error(`Error: ${e.message}`, e)
  process.exit(1)
})
