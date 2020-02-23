const Broker = require('../index')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const { KafkaJSProtocolError } = require('../../errors')
const {
  secureRandom,
  createTopic,
  createConnection,
  newLogger,
  retryProtocol,
} = require('testHelpers')

describe('Broker > AddPartitionsToTxn', () => {
  let broker, seedBroker, transactionalId, producerId, producerEpoch, topicName

  beforeEach(async () => {
    transactionalId = `producer-group-id-${secureRandom()}`
    topicName = `test-topic-${secureRandom()}`

    seedBroker = new Broker({
      connection: createConnection(),
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

    broker = new Broker({
      connection: createConnection({ host, port }),
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
    await seedBroker.disconnect()
    await broker.disconnect()
  })

  test('request', async () => {
    const result = await broker.addPartitionsToTxn({
      transactionalId,
      producerId,
      producerEpoch,
      topics: [
        {
          topic: topicName,
          partitions: [1, 2],
        },
      ],
    })

    expect(result).toEqual({
      throttleTime: 0,
      errors: [
        {
          topic: topicName,
          partitionErrors: [
            { errorCode: 0, partition: 1 },
            { errorCode: 0, partition: 2 },
          ],
        },
      ],
    })
  })

  test('throws for invalid producer id', async () => {
    await expect(
      broker.addPartitionsToTxn({
        transactionalId,
        producerId: 'foo',
        producerEpoch,
        topics: [
          {
            topic: topicName,
            partitions: [1, 2],
          },
        ],
      })
    ).rejects.toEqual(
      new KafkaJSProtocolError(
        'The producer attempted to use a producer id which is not currently assigned to its transactional id'
      )
    )
  })
})
