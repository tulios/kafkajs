const Broker = require('../index')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const {
  secureRandom,
  createTopic,
  createConnectionPool,
  newLogger,
  retryProtocol,
} = require('testHelpers')

describe('Broker > TxnOffsetCommit', () => {
  let seedBroker,
    consumerBroker,
    transactionBroker,
    topicName,
    consumerGroupId,
    transactionalId,
    producerId,
    producerEpoch

  async function findBrokerForGroupId(groupId, coordinatorType) {
    const {
      coordinator: { host, port },
    } = await retryProtocol(
      'GROUP_COORDINATOR_NOT_AVAILABLE',
      async () =>
        await seedBroker.findGroupCoordinator({
          groupId,
          coordinatorType,
        })
    )

    return new Broker({
      connectionPool: createConnectionPool({ host, port }),
      logger: newLogger(),
    })
  }

  beforeEach(async () => {
    transactionalId = `transaction-id-${secureRandom()}`
    consumerGroupId = `group-id-${secureRandom()}`
    topicName = `test-topic-${secureRandom()}`

    seedBroker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })

    await seedBroker.connect()
    await createTopic({ topic: topicName, partitions: 4 })

    transactionBroker = await findBrokerForGroupId(transactionalId, COORDINATOR_TYPES.TRANSACTION)
    await transactionBroker.connect()
    const result = await transactionBroker.initProducerId({
      transactionalId,
      transactionTimeout: 30000,
    })

    producerId = result.producerId
    producerEpoch = result.producerEpoch

    await transactionBroker.addPartitionsToTxn({
      transactionalId,
      producerId,
      producerEpoch,
      topics: [{ topic: topicName, partitions: [0, 1] }],
    })

    await transactionBroker.addOffsetsToTxn({
      transactionalId,
      producerId,
      producerEpoch,
      groupId: consumerGroupId,
    })

    consumerBroker = await findBrokerForGroupId(consumerGroupId, COORDINATOR_TYPES.GROUP)
    await consumerBroker.connect()
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    transactionBroker && (await transactionBroker.disconnect())
    consumerBroker && (await consumerBroker.disconnect())
  })

  test('request', async () => {
    const result = await consumerBroker.txnOffsetCommit({
      transactionalId,
      groupId: consumerGroupId,
      producerId,
      producerEpoch,
      topics: [
        {
          topic: topicName,
          partitions: [
            { partition: 0, offset: 0 },
            { partition: 1, offset: 0 },
          ],
        },
      ],
    })

    result.topics.forEach(topic => topic.partitions.sort((p1, p2) => p1.partition - p2.partition))
    expect(result).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      topics: [
        {
          topic: topicName,
          partitions: [
            { errorCode: 0, partition: 0 },
            { errorCode: 0, partition: 1 },
          ],
        },
      ],
    })
  })

  test('ignores invalid transaction fields', async () => {
    await consumerBroker.txnOffsetCommit({
      transactionalId: 'foo',
      groupId: consumerGroupId,
      producerId: 999,
      producerEpoch: 999,
      topics: [
        {
          topic: topicName,
          partitions: [
            { partition: 0, offset: 0 },
            { partition: 1, offset: 0 },
          ],
        },
      ],
    })
  })
})
