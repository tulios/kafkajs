/**
 * KafkaJS CDC Consumer - Debezium MySQL Connector
 *
 * This example demonstrates how to consume Change Data Capture (CDC) events
 * from a MySQL database using the Debezium connector.
 *
 * Prerequisites:
 *   1. Running Kafka broker
 *   2. Debezium MySQL connector deployed on Kafka Connect
 *   3. MySQL with binlog enabled (binlog_format = ROW)
 *
 * Debezium connector configuration (register via Kafka Connect REST API):
 *
 *   POST http://localhost:8083/connectors
 *   {
 *     "name": "mysql-connector",
 *     "config": {
 *       "connector.class": "io.debezium.connector.mysql.MySqlConnector",
 *       "database.hostname": "localhost",
 *       "database.port": "3306",
 *       "database.user": "debezium",
 *       "database.password": "dbz",
 *       "database.server.id": "184054",
 *       "topic.prefix": "mysqldb",
 *       "database.include.list": "inventory",
 *       "schema.history.internal.kafka.bootstrap.servers": "localhost:9092",
 *       "schema.history.internal.kafka.topic": "schema-changes.inventory"
 *     }
 *   }
 *
 * Topic naming convention: {topic.prefix}.{database}.{table}
 * Example: mysqldb.inventory.customers
 */

const { Kafka, logLevel } = require('../index')
const PrettyConsoleLogger = require('./prettyConsoleLogger')

const host = process.env.HOST_IP || '127.0.0.1'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  logCreator: PrettyConsoleLogger,
  brokers: [`${host}:9092`],
  clientId: 'cdc-mysql-consumer',
})

// Debezium MySQL topics follow the pattern: {topic.prefix}.{database}.{table}
const topics = [
  'mysqldb.inventory.customers',
  'mysqldb.inventory.orders',
  'mysqldb.inventory.products',
]

const consumer = kafka.consumer({ groupId: 'cdc-mysql-handler' })

/**
 * Parse a Debezium MySQL CDC envelope message.
 *
 * Debezium MySQL events have the same envelope structure as PostgreSQL:
 * {
 *   "before": { ... } | null,
 *   "after":  { ... } | null,
 *   "source": {
 *     "connector": "mysql",
 *     "db": "inventory",
 *     "table": "customers",
 *     "file": "mysql-bin.000003",   // MySQL binlog file
 *     "pos": 154,                    // Binlog position
 *     "gtid": null,                  // GTID if enabled
 *     ...
 *   },
 *   "op": "c" | "u" | "d" | "r",
 *   "ts_ms": 1234567890
 * }
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

  return {
    operation: operationMap[value.op] || value.op,
    before: value.before,
    after: value.after,
    source: value.source,
    timestamp: value.ts_ms,
    key,
  }
}

/**
 * Example: Sync CDC events to a data warehouse or search index.
 *
 * MySQL-specific considerations:
 * - Debezium reads the MySQL binlog, so only ROW-format binlog events are captured.
 * - DDL changes (schema changes) are tracked in the schema history topic.
 * - DATETIME/TIMESTAMP columns may be represented as epoch milliseconds.
 */
const handleCDCEvent = async ({ topic, partition, message }) => {
  const event = parseCDCEvent(message)
  const [, database, table] = topic.split('.')

  switch (event.operation) {
    case 'CREATE':
    case 'READ':
      kafka.logger().info(`[${database}.${table}] INSERT`, {
        key: event.key,
        data: event.after,
        binlogFile: event.source?.file,
        binlogPos: event.source?.pos,
      })
      // Example: await elasticsearch.index({ index: table, id: event.key.id, body: event.after })
      break

    case 'UPDATE':
      kafka.logger().info(`[${database}.${table}] UPDATE`, {
        key: event.key,
        before: event.before,
        after: event.after,
        binlogFile: event.source?.file,
        binlogPos: event.source?.pos,
      })
      // Example: await elasticsearch.update({ index: table, id: event.key.id, body: { doc: event.after } })
      break

    case 'DELETE':
      kafka.logger().info(`[${database}.${table}] DELETE`, {
        key: event.key,
        deleted: event.before,
      })
      // Example: await elasticsearch.delete({ index: table, id: event.key.id })
      break

    case 'tombstone':
      kafka.logger().info(`[${database}.${table}] TOMBSTONE`, { key: event.key })
      break

    default:
      kafka.logger().warn(`[${database}.${table}] Unknown operation: ${event.operation}`)
  }
}

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topics, fromBeginning: true })
  await consumer.run({
    eachMessage: handleCDCEvent,
  })
}

run().catch(e => kafka.logger().error(`[example/cdc-mysql] ${e.message}`, { stack: e.stack }))

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
    kafka.logger().info('[example/cdc-mysql] disconnecting')
    await consumer.disconnect()
  })
})
