/**
 * KafkaJS Admin Client Example for Kafka 4.2+ (KRaft mode, no ZooKeeper)
 *
 * Demonstrates admin operations against a KRaft-native Kafka cluster.
 *
 * Usage:
 *   node examples/kraft-admin.js
 *
 * Environment variables:
 *   KAFKA_BROKERS - comma-separated broker list (default: localhost:9092)
 */

const { Kafka, logLevel } = require('../index')

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',')

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers,
  clientId: 'kafkajs-kraft-admin',
})

const admin = kafka.admin()

const run = async () => {
  await admin.connect()
  kafka.logger().info('Admin connected to KRaft cluster', { brokers })

  // 1. Describe the cluster (KRaft mode - no ZooKeeper)
  const clusterInfo = await admin.describeCluster()
  kafka.logger().info('Cluster info', {
    clusterId: clusterInfo.clusterId,
    controller: clusterInfo.controller,
    brokers: clusterInfo.brokers.map(b => `${b.host}:${b.port} (node ${b.nodeId})`),
  })

  // 2. Create a test topic
  const topicName = 'kraft-admin-test-topic'
  const created = await admin.createTopics({
    topics: [
      {
        topic: topicName,
        numPartitions: 3,
        replicationFactor: 2,
        configEntries: [
          { name: 'cleanup.policy', value: 'delete' },
          { name: 'retention.ms', value: '86400000' }, // 24 hours
        ],
      },
    ],
    waitForLeaders: true,
  })
  kafka.logger().info(`Topic '${topicName}' created: ${created}`)

  // 3. List all topics
  const topics = await admin.listTopics()
  kafka.logger().info('Topics', { topics })

  // 4. Fetch topic metadata
  const metadata = await admin.fetchTopicMetadata({ topics: [topicName] })
  kafka.logger().info('Topic metadata', {
    topic: topicName,
    partitions: metadata.topics[0].partitions.length,
  })

  // 5. Add partitions
  await admin.createPartitions({
    topicPartitions: [{ topic: topicName, count: 6 }],
  })
  kafka.logger().info(`Topic '${topicName}' expanded to 6 partitions`)

  // 6. Describe configurations
  const configs = await admin.describeConfigs({
    includeSynonyms: false,
    resources: [{ type: 2, name: topicName }], // type 2 = TOPIC
  })
  kafka.logger().info('Topic configuration', {
    entries: configs.resources[0].configEntries
      .filter(e => !e.isDefault)
      .map(e => `${e.configName}=${e.configValue}`),
  })

  // 7. List consumer groups
  const groups = await admin.listGroups()
  kafka.logger().info('Consumer groups', { groups: groups.groups })

  // 8. Cleanup - delete the test topic
  await admin.deleteTopics({ topics: [topicName] })
  kafka.logger().info(`Topic '${topicName}' deleted`)

  await admin.disconnect()
  kafka.logger().info('Admin disconnected')
}

run().catch(e => {
  kafka.logger().error(`[kraft-admin] ${e.message}`, { stack: e.stack })
  admin.disconnect().then(() => process.exit(1))
})
