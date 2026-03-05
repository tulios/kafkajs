/**
 * KafkaJS CDC Consumer - Debezium MongoDB Connector
 *
 * This example demonstrates how to consume Change Data Capture (CDC) events
 * from a MongoDB database using the Debezium connector.
 *
 * Prerequisites:
 *   1. Running Kafka broker
 *   2. Debezium MongoDB connector deployed on Kafka Connect
 *   3. MongoDB replica set (required for change streams)
 *
 * Debezium connector configuration (register via Kafka Connect REST API):
 *
 *   POST http://localhost:8083/connectors
 *   {
 *     "name": "mongodb-connector",
 *     "config": {
 *       "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
 *       "mongodb.connection.string": "mongodb://localhost:27017/?replicaSet=rs0",
 *       "topic.prefix": "mongodb",
 *       "collection.include.list": "inventory.customers,inventory.orders",
 *       "capture.mode": "change_streams_update_full"
 *     }
 *   }
 *
 * Topic naming convention: {topic.prefix}.{database}.{collection}
 * Example: mongodb.inventory.customers
 *
 * Note: MongoDB Debezium events differ from relational databases.
 * Documents are represented as JSON strings rather than flat columns.
 */

const { Kafka, logLevel } = require('../index')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

const host = process.env.HOST_IP || '127.0.0.1'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  logCreator: PrettyConsoleLogger,
  brokers: [`${host}:9092`],
  clientId: 'cdc-mongodb-consumer',
})

// Debezium MongoDB topics follow the pattern: {topic.prefix}.{database}.{collection}
const topics = ['mongodb.inventory.customers', 'mongodb.inventory.orders']

const consumer = kafka.consumer({ groupId: 'cdc-mongodb-handler' })

/**
 * Parse a Debezium MongoDB CDC event.
 *
 * MongoDB events differ from relational DB events:
 *
 * For inserts (op: "c"):
 *   { "after": "{\"_id\": ..., \"name\": \"John\"}", "source": {...}, "op": "c" }
 *
 * For updates (op: "u") with capture.mode=change_streams_update_full:
 *   { "before": null, "after": "{...full document...}", "updateDescription": {...}, "op": "u" }
 *
 * For deletes (op: "d"):
 *   { "before": "{\"_id\": ...}", "after": null, "op": "d" }
 *
 * The "after" and "before" fields are JSON-encoded strings (not objects).
 */
const parseCDCEvent = message => {
  const value = message.value ? JSON.parse(message.value.toString()) : null
  const key = message.key ? JSON.parse(message.key.toString()) : null

  if (!value) {
    return { operation: 'tombstone', key, before: null, after: null, source: null }
  }

  const operationMap = {
    c: 'CREATE',
    u: 'UPDATE',
    d: 'DELETE',
    r: 'READ',
  }

  // MongoDB Debezium wraps documents as JSON strings
  const parseDocument = doc => {
    if (!doc) return null
    return typeof doc === 'string' ? JSON.parse(doc) : doc
  }

  return {
    operation: operationMap[value.op] || value.op,
    before: parseDocument(value.before),
    after: parseDocument(value.after),
    updateDescription: value.updateDescription || null,
    source: value.source,
    timestamp: value.ts_ms,
    key,
  }
}

/**
 * Example: Replicate MongoDB changes to another data store.
 *
 * MongoDB-specific considerations:
 * - Documents may have nested structures unlike flat relational rows.
 * - The _id field is always part of the key.
 * - With "change_streams_update_full" mode, full documents are included on updates.
 * - updateDescription contains "updatedFields" and "removedFields" for partial changes.
 */
const handleCDCEvent = async ({ topic, partition, message }) => {
  const event = parseCDCEvent(message)
  const [, database, collection] = topic.split('.')

  switch (event.operation) {
    case 'CREATE':
    case 'READ':
      kafka.logger().info(`[${database}.${collection}] INSERT`, {
        id: event.after?._id,
        document: event.after,
      })
      // Example: await targetDb.collection(collection).insertOne(event.after)
      break

    case 'UPDATE': {
      kafka.logger().info(`[${database}.${collection}] UPDATE`, {
        id: event.key,
        fullDocument: event.after,
        updatedFields: event.updateDescription?.updatedFields,
        removedFields: event.updateDescription?.removedFields,
      })
      // Example: await targetDb.collection(collection).replaceOne({ _id: event.key.id }, event.after)
      break
    }

    case 'DELETE':
      kafka.logger().info(`[${database}.${collection}] DELETE`, {
        id: event.key,
        deleted: event.before,
      })
      // Example: await targetDb.collection(collection).deleteOne({ _id: event.key.id })
      break

    case 'tombstone':
      kafka.logger().info(`[${database}.${collection}] TOMBSTONE`, { key: event.key })
      break

    default:
      kafka.logger().warn(`[${database}.${collection}] Unknown operation: ${event.operation}`)
  }
}

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topics, fromBeginning: true })
  await consumer.run({
    eachMessage: handleCDCEvent,
  })
}

run().catch(e => kafka.logger().error(`[example/cdc-mongodb] ${e.message}`, { stack: e.stack }))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      kafka.logger().info(`process.on ${type}`)
      kafka.logger().error(e.message, { stack: e.stack })
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    console.log('')
    kafka.logger().info('[example/cdc-mongodb] disconnecting')
    await consumer.disconnect()
  })
})
