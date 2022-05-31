const Broker = require('../index')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const { secureRandom, createConnectionPool, newLogger, retryProtocol } = require('testHelpers')
const { KafkaJSProtocolError } = require('../../errors')

describe('Broker > AddOffsetsToTxn', () => {
  let broker, seedBroker, transactionalId, producerId, producerEpoch, consumerGroupId

  beforeEach(async () => {
    transactionalId = `transactional-id-${secureRandom()}`
    consumerGroupId = `group-id-${secureRandom()}`

    seedBroker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })

    await seedBroker.connect()

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

    broker = new Broker({
      connectionPool: createConnectionPool({ host, port }),
      logger: newLogger(),
    })

    await broker.connect()
    const result = await broker.initProducerId({
      transactionalId,
      transactionTimeout: 30000,
    })

    producerId = result.producerId
    producerEpoch = result.producerEpoch
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
  })

  test('request', async () => {
    const result = await broker.addOffsetsToTxn({
      transactionalId,
      producerId,
      producerEpoch,
      groupId: consumerGroupId,
    })

    expect(result).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      errorCode: 0,
    })
  })

  test('throws for incorrect producer id', async () => {
    await expect(
      broker.addOffsetsToTxn({
        transactionalId,
        producerId: 12345,
        producerEpoch,
        groupId: consumerGroupId,
      })
    ).rejects.toEqual(
      new KafkaJSProtocolError(
        'The producer attempted to use a producer id which is not currently assigned to its transactional id'
      )
    )
  })

  test('throws for incorrect producer epoch', async () => {
    await expect(
      broker.addOffsetsToTxn({
        transactionalId,
        producerId,
        producerEpoch: producerEpoch + 1,
        groupId: consumerGroupId,
      })
    ).rejects.toEqual(
      new KafkaJSProtocolError(
        "Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker"
      )
    )
  })
})
