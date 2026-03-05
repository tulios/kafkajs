/**
 * KafkaJS CDC Consumer - Debezium PostgreSQL Connector
 *
 * This example demonstrates how to consume Change Data Capture (CDC) events
 * from a PostgreSQL database using the Debezium connector.
 *
 * Prerequisites:
 *   1. Running Kafka broker
 *   2. Debezium PostgreSQL connector deployed on Kafka Connect
 *   3. PostgreSQL with logical replication enabled (wal_level = logical)
 *
 * Debezium connector configuration (register via Kafka Connect REST API):
 *
 *   POST http://localhost:8083/connectors
 *   {
 *     "name": "postgres-connector",
 *     "config": {
 *       "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
 *       "database.hostname": "localhost",
 *       "database.port": "5432",
 *       "database.user": "debezium",
 *       "database.password": "dbz",
 *       "database.dbname": "inventory",
 *       "topic.prefix": "dbserver1",
 *       "schema.include.list": "public",
 *       "plugin.name": "pgoutput"
 *     }
 *   }
 *
 * Topic naming convention: {topic.prefix}.{schema}.{table}
 * Example: dbserver1.public.customers
 */

const { Kafka, logLevel } = require('../index')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

const host = process.env.HOST_IP || '127.0.0.1'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  logCreator: PrettyConsoleLogger,
  brokers: [`${host}:9092`],
  clientId: 'cdc-postgres-consumer',
})

// Debezium topics follow the pattern: {topic.prefix}.{schema}.{table}
const topics = [
  'dbserver1.public.customers',
  'dbserver1.public.orders',
  'dbserver1.public.products',
]

const consumer = kafka.consumer({ groupId: 'cdc-postgres-handler' })

/**
 * Parse a Debezium CDC envelope message.
 *
 * Debezium PostgreSQL events have the following structure:
 * {
 *   "before": { ... } | null,   // Row state before the change (null for inserts)
 *   "after":  { ... } | null,   // Row state after the change (null for deletes)
 *   "source": { ... },          // Metadata about the source database/table/LSN
 *   "op": "c" | "u" | "d" | "r", // Operation: create, update, delete, read (snapshot)
 *   "ts_ms": 1234567890         // Timestamp of the change event
 * }
 */
const parseCDCEvent = message => {
  const value = message.value ? JSON.parse(message.value.toString()) : null
  const key = message.key ? JSON.parse(message.key.toString()) : null

  if (!value) {
    // Tombstone event (used by Kafka log compaction to remove deleted records)
    return { operation: 'tombstone', key, before: null, after: null, source: null }
  }

  const operationMap = {
    c: 'CREATE',
    u: 'UPDATE',
    d: 'DELETE',
    r: 'READ', // Snapshot event
  }

  return {
    operation: operationMap[value.op] || value.op,
    before: value.before,
    after: value.after,
    source: value.source,
    timestamp: value.ts_ms,
    key,
  }
}

const handleCDCEvent = async ({ topic, partition, message }) => {
  const event = parseCDCEvent(message)
  const tableName = topic.split('.').pop()

  switch (event.operation) {
    case 'CREATE':
    case 'READ':
      kafka.logger().info(`[${tableName}] INSERT`, {
        key: event.key,
        data: event.after,
        lsn: event.source?.lsn,
      })
      // Example: await db.upsert(tableName, event.after)
      break

    case 'UPDATE':
      kafka.logger().info(`[${tableName}] UPDATE`, {
        key: event.key,
        before: event.before,
        after: event.after,
        lsn: event.source?.lsn,
      })
      // Example: await db.upsert(tableName, event.after)
      break

    case 'DELETE':
      kafka.logger().info(`[${tableName}] DELETE`, {
        key: event.key,
        deleted: event.before,
        lsn: event.source?.lsn,
      })
      // Example: await db.delete(tableName, event.key)
      break

    case 'tombstone':
      kafka.logger().info(`[${tableName}] TOMBSTONE`, { key: event.key })
      break

    default:
      kafka.logger().warn(`[${tableName}] Unknown operation: ${event.operation}`)
  }
}

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topics, fromBeginning: true })
  await consumer.run({
    eachMessage: handleCDCEvent,
  })
}

run().catch(e => kafka.logger().error(`[example/cdc-postgres] ${e.message}`, { stack: e.stack }))

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
    kafka.logger().info('[example/cdc-postgres] disconnecting')
    await consumer.disconnect()
  })
})
