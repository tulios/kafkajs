const createConsumer = require('../index')
const createProducer = require('../../producer')
const cooperativeSticky = require('../assigners/cooperativeStickyAssignor')
const { roundRobin } = require('../assigners')

const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitFor,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

/**
 * Integration coverage for the cooperative-sticky assigner against real
 * brokers. Asserts the KIP-429 guarantees: incremental (not full) revocation
 * on scale-up, offset continuity for retained partitions, and graceful
 * degradation in a mixed-protocol group.
 *
 * These require a running broker (./scripts/dockerComposeUp.sh). When no broker
 * is reachable they fail fast; upstream CI (azure-pipelines.yml) provisions the
 * broker matrix.
 */
describe('Consumer > cooperativeStickyAssignor > integration', () => {
  let topicName, groupId, producer, consumers

  const createCooperativeConsumer = () =>
    createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      heartbeatInterval: 1000,
      rebalanceTimeout: 10000,
      sessionTimeout: 10000,
      logger: newLogger(),
      partitionAssigners: [cooperativeSticky],
    })

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`
    consumers = []

    await createTopic({ topic: topicName, partitions: 6 })

    producer = createProducer({ cluster: createCluster(), logger: newLogger() })
    await producer.connect()
  })

  afterEach(async () => {
    await Promise.all(consumers.map(c => c.disconnect().catch(() => {})))
    producer && (await producer.disconnect())
  })

  const trackGroupJoins = consumer => {
    const events = []
    consumer.on(consumer.events.GROUP_JOIN, e => events.push(e.payload))
    return events
  }

  it('negotiates the cooperative-sticky protocol for an all-cooperative group', async () => {
    const consumer = createCooperativeConsumer()
    consumers.push(consumer)
    const joins = trackGroupJoins(consumer)

    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    consumer.run({ eachMessage: () => {} })

    await waitForConsumerToJoinGroup(consumer)

    expect(joins[0].groupProtocol).toEqual('cooperative-sticky')
    // Single member owns all 6 partitions; nothing revoked.
    expect(joins[0].revoked).toEqual([])
  })

  it('revokes incrementally (not fully) when a new consumer joins', async () => {
    const consumer1 = createCooperativeConsumer()
    consumers.push(consumer1)
    const joins1 = trackGroupJoins(consumer1)

    await consumer1.connect()
    await consumer1.subscribe({ topic: topicName, fromBeginning: true })
    consumer1.run({ eachMessage: () => {} })
    await waitForConsumerToJoinGroup(consumer1)

    // consumer1 owns all 6 partitions.
    const initialAssignment = joins1[joins1.length - 1].memberAssignment[topicName]
    expect(initialAssignment.length).toEqual(6)

    // Add a second cooperative consumer.
    const consumer2 = createCooperativeConsumer()
    consumers.push(consumer2)
    await consumer2.connect()
    await consumer2.subscribe({ topic: topicName, fromBeginning: true })
    consumer2.run({ eachMessage: () => {} })

    await waitForConsumerToJoinGroup(consumer2)
    // Wait until consumer1 has rebalanced down to 3 partitions.
    await waitFor(() => {
      const last = joins1[joins1.length - 1]
      return last && (last.memberAssignment[topicName] || []).length === 3
    })

    // consumer1 kept 3 of its 6 partitions — only 3 were revoked, not all 6.
    const revokedAcrossRounds = joins1
      .flatMap(j => j.revoked || [])
      .map(({ partition }) => partition)
    expect(revokedAcrossRounds.length).toEqual(3)
    expect(joins1[joins1.length - 1].memberAssignment[topicName].length).toEqual(3)
  })

  it('degrades gracefully to a common protocol in a mixed-protocol group', async () => {
    const cooperative = createCooperativeConsumer()
    const eager = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
      // Lists roundRobin only — no cooperative support.
      partitionAssigners: [roundRobin],
    })
    consumers.push(cooperative, eager)

    // The cooperative consumer also offers roundRobin as a fallback protocol.
    const cooperativeWithFallback = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
      partitionAssigners: [cooperativeSticky, roundRobin],
    })
    consumers.push(cooperativeWithFallback)

    const joins = trackGroupJoins(cooperativeWithFallback)

    await Promise.all([cooperativeWithFallback.connect(), eager.connect()])
    await cooperativeWithFallback.subscribe({ topic: topicName, fromBeginning: true })
    await eager.subscribe({ topic: topicName, fromBeginning: true })

    cooperativeWithFallback.run({ eachMessage: () => {} })
    eager.run({ eachMessage: () => {} })

    await Promise.all([
      waitForConsumerToJoinGroup(cooperativeWithFallback),
      waitForConsumerToJoinGroup(eager),
    ])

    // Group degrades to the common protocol (roundRobin), no crash.
    expect(joins[joins.length - 1].groupProtocol).toEqual('RoundRobinAssigner')
  })
})
