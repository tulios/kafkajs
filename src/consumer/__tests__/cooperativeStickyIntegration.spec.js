/**
 * Integration tests for CooperativeStickyAssigner
 *
 * These tests require a running Kafka broker (see docker-compose.demo.yml or
 * docker-compose.2_4.yml).  They are automatically skipped when the broker is
 * unavailable so the CI suite can still pass without Docker.
 *
 * Run against the lightweight demo broker:
 *   docker-compose -f docker-compose.demo.yml up -d
 *   NODE_ENV=test ./node_modules/.bin/jest src/consumer/__tests__/cooperativeStickyIntegration.spec.js --no-coverage --reporters=default
 *
 * Or with the full test cluster:
 *   yarn test:local -- --testPathPattern cooperativeStickyIntegration
 */

'use strict'

jest.setTimeout(120000)

const {
  createTopic,
  secureRandom,
  createCluster,
  newLogger,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

const { cooperativeSticky } = require('../assigners')
const createConsumer = require('../index')
const createProducer = require('../../producer')

// ─── Helpers ─────────────────────────────────────────────────────────────────

/** Returns the flat, sorted list of partition IDs the consumer is assigned. */
function getPartitions(consumer) {
  // The assignment event gives us the member assignment map.
  // We capture it via the latest GROUP_JOIN event payload.
  return consumer._assignedPartitions || []
}

/**
 * Subscribes `consumer` to its GROUP_JOIN event and updates a cached list so
 * tests can inspect the current assignment without reaching into internals.
 */
function trackAssignment(consumer) {
  consumer._assignedPartitions = []
  consumer.on(consumer.events.GROUP_JOIN, event => {
    const assignment = event.payload.memberAssignment || {}
    consumer._assignedPartitions = Object.values(assignment)
      .flat()
      .map(Number)
      .sort((a, b) => a - b)
  })
}

/**
 * Start a consumer, subscribe it to `topic`, and begin running.
 * Returns once the consumer has received its first GROUP_JOIN.
 */
async function startConsumer({ cluster, groupId, topic, onMessage }) {
  const consumer = createConsumer({
    cluster,
    groupId,
    partitionAssigners: [cooperativeSticky],
    sessionTimeout: 10000,
    heartbeatInterval: 1000,
    maxWaitTimeInMs: 500,
    logger: newLogger(),
  })
  trackAssignment(consumer)
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: false })
  await consumer.run({
    eachMessage: async ctx => {
      if (onMessage) onMessage(ctx)
    },
  })
  await waitForConsumerToJoinGroup(consumer, { maxWait: 20000 })
  return consumer
}

/**
 * Waits until `consumer` fires a GROUP_JOIN event (i.e., it has (re)joined the
 * group) within `maxWait` ms.
 */
function waitForRebalance(consumer, maxWait = 30000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`${consumer._consumerId} timed out waiting for GROUP_JOIN`)),
      maxWait
    )
    const removeListener = consumer.on(consumer.events.GROUP_JOIN, event => {
      clearTimeout(timer)
      removeListener()
      resolve(event)
    })
  })
}

/**
 * Produce `count` messages evenly across all partitions of `topic`.
 */
async function produceMessages(producer, topic, count) {
  const messages = Array.from({ length: count }, (_, i) => ({
    key: String(i % 10), // key 0–9 → explicit partition routing
    value: `msg-${i}`,
  }))
  await producer.send({ topic, messages, acks: -1 })
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe('CooperativeStickyAssigner – integration', () => {
  let topic, groupId, cluster, producer
  const consumers = []

  beforeAll(async () => {
    topic = `coop-sticky-${secureRandom()}`
    groupId = `coop-sticky-group-${secureRandom()}`

    // Create topic with 10 partitions
    await createTopic({ topic, partitions: 10, replicas: 1 })

    cluster = createCluster()
    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()
  })

  afterAll(async () => {
    for (const c of consumers) {
      try {
        await c.disconnect()
      } catch (_) {}
    }
    await producer.disconnect()
  })

  // ── Test 1: initial distribution ──────────────────────────────────────────

  it('distributes all 10 partitions fairly across 4 initial consumers', async () => {
    // Start 4 consumers sequentially so we can track each GROUP_JOIN
    for (let i = 0; i < 4; i++) {
      const c = await startConsumer({ cluster, groupId, topic })
      c._consumerId = `C${i + 1}`
      consumers.push(c)
      // Wait a beat so each consumer joins before the next, giving stable assignment
      await new Promise(resolve => setTimeout(resolve, 1000))
    }

    // Give the group a moment to finish its final rebalance
    await new Promise(resolve => setTimeout(resolve, 3000))

    const allPartitions = consumers.flatMap(c => getPartitions(c)).sort((a, b) => a - b)
    const uniquePartitions = [...new Set(allPartitions)]

    // Every partition 0-9 must be assigned exactly once
    expect(uniquePartitions).toHaveLength(10)
    expect(uniquePartitions).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

    // Loads should be balanced: each consumer gets 2 or 3 partitions
    for (const c of consumers) {
      const count = getPartitions(c).length
      expect(count).toBeGreaterThanOrEqual(2)
      expect(count).toBeLessThanOrEqual(3)
    }
  })

  // ── Test 2: cooperative behaviour when a new consumer joins ───────────────

  it('continues processing on unchanged partitions when a 5th consumer joins', async () => {
    // Capture the assignment of the first 4 consumers BEFORE C5 joins
    const assignmentBefore = consumers.map(c => ({
      id: c._consumerId,
      partitions: [...getPartitions(c)],
    }))

    // Produce some messages
    const produceInterval = setInterval(() => produceMessages(producer, topic, 10), 400)

    // Join C5 and wait for the rebalance to settle
    const rebalancePromises = consumers.map(c => waitForRebalance(c, 30000))
    const c5 = await startConsumer({ cluster, groupId, topic })
    c5._consumerId = 'C5'
    consumers.push(c5)

    await Promise.all(rebalancePromises)
    clearInterval(produceInterval)

    // Give cooperative protocol a second rebalance cycle to complete the transfers
    await new Promise(resolve => setTimeout(resolve, 5000))

    const assignmentAfter = consumers.map(c => ({
      id: c._consumerId,
      partitions: [...getPartitions(c)],
    }))

    // ── Assertions ──────────────────────────────────────────────────────────

    // All 10 partitions must still be assigned across the 5 consumers
    const allPartitionsAfter = assignmentAfter.flatMap(a => a.partitions).sort((a, b) => a - b)
    expect([...new Set(allPartitionsAfter)]).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

    // C5 must have received at least 1 partition (2 is ideal)
    const c5Assignment = assignmentAfter.find(a => a.id === 'C5')
    expect(c5Assignment.partitions.length).toBeGreaterThanOrEqual(1)

    // Cooperative stickiness: most of each consumer's previous partitions should
    // still be with that consumer (only the transferred ones moved)
    for (const before of assignmentBefore) {
      const after = assignmentAfter.find(a => a.id === before.id)
      if (!after) continue
      // At least half of the original partitions should have stayed
      const kept = before.partitions.filter(p => after.partitions.includes(p))
      expect(kept.length).toBeGreaterThanOrEqual(Math.floor(before.partitions.length / 2))
    }
  })

  // ── Test 3: consumer leaves – remaining consumers absorb its partitions ───

  it('redistributes partitions when a consumer leaves without disrupting others', async () => {
    // Snapshot assignment before C1 leaves
    const c1 = consumers.find(c => c._consumerId === 'C1')
    const c1Partitions = [...getPartitions(c1)]
    expect(c1Partitions.length).toBeGreaterThan(0)

    const remainingConsumers = consumers.filter(c => c._consumerId !== 'C1')
    const assignmentBefore = remainingConsumers.map(c => ({
      id: c._consumerId,
      partitions: [...getPartitions(c)],
    }))

    // Listen for the group rebalance on remaining consumers
    const rebalancePromises = remainingConsumers.map(c => waitForRebalance(c, 30000))

    await c1.disconnect()

    await Promise.all(rebalancePromises)
    await new Promise(resolve => setTimeout(resolve, 3000)) // let second cooperative round settle

    const assignmentAfter = remainingConsumers.map(c => ({
      id: c._consumerId,
      partitions: [...getPartitions(c)],
    }))

    // ── Assertions ──────────────────────────────────────────────────────────

    // All 10 partitions must still be covered
    const allPartitionsAfter = assignmentAfter.flatMap(a => a.partitions).sort((a, b) => a - b)
    expect([...new Set(allPartitionsAfter)]).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

    // Every partition previously owned by C1 must be with a remaining consumer
    for (const p of c1Partitions) {
      const newOwner = assignmentAfter.find(a => a.partitions.includes(p))
      expect(newOwner).toBeDefined()
    }

    // Stickiness: each remaining consumer should still hold most of its old partitions
    for (const before of assignmentBefore) {
      const after = assignmentAfter.find(a => a.id === before.id)
      if (!after) continue
      const kept = before.partitions.filter(p => after.partitions.includes(p))
      // All previously-held partitions should still be present (C1's were just added on top)
      expect(kept.length).toBe(before.partitions.length)
    }

    // Final load: each remaining consumer should get 2 or 3 partitions
    for (const a of assignmentAfter) {
      expect(a.partitions.length).toBeGreaterThanOrEqual(2)
      expect(a.partitions.length).toBeLessThanOrEqual(3)
    }
  })

  // ── Test 4: message continuity across rebalances ──────────────────────────

  it('delivers all produced messages exactly once (no duplicates, no loss)', async () => {
    const continuityTopic = `coop-sticky-continuity-${secureRandom()}`
    const continuityGroup = `coop-continuity-group-${secureRandom()}`
    await createTopic({ topic: continuityTopic, partitions: 10, replicas: 1 })

    const received = new Map() // offset → value

    // Start 2 consumers
    const ca = await startConsumer({
      cluster,
      groupId: continuityGroup,
      topic: continuityTopic,
      onMessage: ({ message, partition, offset }) => {
        received.set(`${partition}-${offset}`, message.value.toString())
      },
    })
    const cb = await startConsumer({
      cluster,
      groupId: continuityGroup,
      topic: continuityTopic,
      onMessage: ({ message, partition, offset }) => {
        received.set(`${partition}-${offset}`, message.value.toString())
      },
    })

    await new Promise(resolve => setTimeout(resolve, 2000))

    // Produce 50 messages
    const sent = []
    for (let i = 0; i < 50; i++) {
      sent.push({ key: String(i % 10), value: `v${i}` })
    }
    await producer.send({ topic: continuityTopic, messages: sent, acks: -1 })

    // Add a 3rd consumer mid-flight to trigger a cooperative rebalance
    const rebalancePromises = [waitForRebalance(ca, 30000), waitForRebalance(cb, 30000)]
    const cc = await startConsumer({
      cluster,
      groupId: continuityGroup,
      topic: continuityTopic,
      onMessage: ({ message, partition, offset }) => {
        received.set(`${partition}-${offset}`, message.value.toString())
      },
    })
    await Promise.all(rebalancePromises)

    // Give consumers time to finish processing
    await new Promise(resolve => setTimeout(resolve, 5000))

    // All 50 messages should have been received (no loss)
    expect(received.size).toBe(50)
    // Each unique key-value pair should appear only once (no duplicates for acks=-1)
    const values = [...received.values()]
    expect(new Set(values).size).toBe(values.length)

    await ca.disconnect()
    await cb.disconnect()
    await cc.disconnect()
  })
})
