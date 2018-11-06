const Broker = require('../index')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const {
  secureRandom,
  createTopic,
  createConnection,
  newLogger,
  retryProtocol,
} = require('testHelpers')

describe('Broker > TxnOffsetCommit', () => {
  let seedBroker, consumerBroker, topicName, consumerGroupId

  async function findBrokerForGroupId(groupId) {
    const {
      coordinator: { host, port },
    } = await retryProtocol(
      'GROUP_COORDINATOR_NOT_AVAILABLE',
      async () =>
        await seedBroker.findGroupCoordinator({
          groupId,
          coordinatorType: COORDINATOR_TYPES.TRANSACTION,
        })
    )

    return new Broker({
      connection: createConnection({ host, port }),
      logger: newLogger(),
    })
  }

  beforeEach(async () => {
    consumerGroupId = `group-id-${secureRandom()}`
    topicName = `test-topic-${secureRandom()}`

    seedBroker = new Broker({
      connection: createConnection(),
      logger: newLogger(),
    })

    await seedBroker.connect()
    await createTopic({ topic: topicName, partitions: 4 })

    consumerBroker = await findBrokerForGroupId(consumerGroupId)
    await consumerBroker.connect()
  })

  afterEach(async () => {
    await seedBroker.disconnect()
    await consumerBroker.disconnect()
  })

  test('request', async () => {
    const result = await consumerBroker.txnOffsetCommit({
      transactionalId: 'test-transaction-id',
      groupId: consumerGroupId,
      producerId: 999,
      producerEpoch: 999,
      topics: [
        {
          topic: topicName,
          partitions: [{ partition: 1, offset: 0 }, { partition: 2, offset: 0 }],
        },
      ],
    })

    expect(result).toEqual({
      throttleTime: 0,
      topics: [
        {
          topic: topicName,
          partitions: [{ errorCode: 0, partition: 1 }, { errorCode: 0, partition: 2 }],
        },
      ],
    })
  })
})
