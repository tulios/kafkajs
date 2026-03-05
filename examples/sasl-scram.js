/**
 * Example: SASL SCRAM-SHA-256 / SCRAM-SHA-512 Authentication
 *
 * SCRAM (Salted Challenge Response Authentication Mechanism) provides
 * secure password-based authentication without sending passwords in cleartext.
 * Uses PBKDF2 for key derivation with a multi-step challenge-response handshake.
 *
 * Supports both SHA-256 and SHA-512 digest algorithms.
 *
 * Broker configuration (server.properties):
 *   sasl.enabled.mechanisms=SCRAM-SHA-256,SCRAM-SHA-512
 *
 * Create SCRAM credentials on the broker:
 *   kafka-configs.sh --bootstrap-server localhost:9092 \
 *     --alter --add-config 'SCRAM-SHA-256=[password=secret],SCRAM-SHA-512=[password=secret]' \
 *     --entity-type users --entity-name my-user
 *
 * Usage:
 *   SASL_MECHANISM=scram-sha-256 \
 *   SASL_USERNAME=my-user \
 *   SASL_PASSWORD=secret \
 *   KAFKA_BROKERS=kafka1:9092,kafka2:9092 \
 *   node examples/sasl-scram.js
 */

const { Kafka, logLevel } = require('../index')

const mechanism = process.env.SASL_MECHANISM || 'scram-sha-256' // or 'scram-sha-512'

const kafka = new Kafka({
  clientId: 'sasl-scram-example',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  logLevel: logLevel.INFO,
  ssl: process.env.SSL === 'true' || undefined,
  sasl: {
    mechanism,
    username: process.env.SASL_USERNAME || 'test',
    password: process.env.SASL_PASSWORD || 'testtest',
  },
})

const run = async () => {
  const admin = kafka.admin()
  await admin.connect()
  console.log(`Connected with SASL ${mechanism.toUpperCase()} authentication`)

  const topics = await admin.listTopics()
  console.log('Topics:', topics)

  await admin.disconnect()
}

run().catch(e => {
  console.error(`Error: ${e.message}`, e)
  process.exit(1)
})
