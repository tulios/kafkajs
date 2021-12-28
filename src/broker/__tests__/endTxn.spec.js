const Broker = require('../index')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const {
  secureRandom,
  createConnectionPool,
  newLogger,
  retryProtocol,
  createTopic,
} = require('testHelpers')
const { KafkaJSProtocolError } = require('../../errors')

describe('Broker > EndTxn', () => {
  let transactionBroker, seedBroker, transactionalId, producerId, producerEpoch, topicName

  function addPartitionsToTxn() {
    return transactionBroker.addOffsetsToTxn({
      transactionalId,
      producerId,
      producerEpoch,
      groupId: `group-id-${secureRandom()}`,
    })
  }

  beforeEach(async () => {
    transactionalId = `transactional-id-${secureRandom()}`
    topicName = `test-topic-${secureRandom()}`

    seedBroker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })

    await seedBroker.connect()
    await createTopic({ topic: topicName, partitions: 4 })

    const {
      coordinator: { host, port },
    } = await retryProtocol(
      'GROUP_COORDINATOR_NOT_AVAILABLE',
      async () =>
        await seedBroker.findGroupCoordinator({
          groupId: transactionalId,
          coordinatorType: COORDINATOR_TYPES.TRANSACTION,
        })
    )

    transactionBroker = new Broker({
      connectionPool: createConnectionPool({ host, port }),
      logger: newLogger(),
    })

    await transactionBroker.connect()

    const result = await transactionBroker.initProducerId({
      transactionalId,
      transactionTimeout: 30000,
    })

    producerId = result.producerId
    producerEpoch = result.producerEpoch
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    transactionBroker && (await transactionBroker.disconnect())
  })

  test('commit transaction', async () => {
    await addPartitionsToTxn()

    const result = await transactionBroker.endTxn({
      transactionalId,
      producerId,
      producerEpoch,
      transactionResult: true, // Commit
    })

    expect(result).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      errorCode: 0,
    })
  })

  test('abort transaction', async () => {
    await addPartitionsToTxn()

    const result = await transactionBroker.endTxn({
      transactionalId,
      producerId,
      producerEpoch,
      transactionResult: false, // Abort
    })

    expect(result).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      errorCode: 0,
    })
  })

  test('throws if transaction is in invalid state (eg have not yet added partitions to transaction)', async () => {
    await expect(
      transactionBroker.endTxn({
        transactionalId,
        producerId,
        producerEpoch,
        transactionResult: true,
      })
    ).rejects.toEqual(
      new KafkaJSProtocolError(
        'The producer attempted a transactional operation in an invalid state'
      )
    )
  })

  test('throws for incorrect transactional id', async () => {
    await addPartitionsToTxn()

    let error

    try {
      await transactionBroker.endTxn({
        transactionalId: 'foobar',
        producerId,
        producerEpoch,
        transactionResult: true,
      })
    } catch (e) {
      error = e
    }

    expect(error).toBeDefined()
    expect([
      new KafkaJSProtocolError(
        'The producer attempted to use a producer id which is not currently assigned to its transactional id'
      ),
      new KafkaJSProtocolError('This is not the correct coordinator for this group'),
    ]).toContainEqual(error)
  })

  test('throws for incorrect producer id', async () => {
    await addPartitionsToTxn()

    await expect(
      transactionBroker.endTxn({
        transactionalId,
        producerId: 12345,
        producerEpoch,
        transactionResult: true,
      })
    ).rejects.toEqual(
      new KafkaJSProtocolError(
        'The producer attempted to use a producer id which is not currently assigned to its transactional id'
      )
    )
  })

  test('throws for incorrect producer epoch', async () => {
    await addPartitionsToTxn()

    await expect(
      transactionBroker.endTxn({
        transactionalId,
        producerId,
        producerEpoch: producerEpoch + 1,
        transactionResult: true,
      })
    ).rejects.toEqual(
      new KafkaJSProtocolError(
        "Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker"
      )
    )
  })
})
