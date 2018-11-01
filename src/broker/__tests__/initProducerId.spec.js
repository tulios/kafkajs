const Broker = require('../index')
const { createConnection, newLogger, retryProtocol, secureRandom } = require('testHelpers')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const { KafkaJSProtocolError } = require('../../errors')

describe('Broker > InitProducerId', () => {
  let broker, seedBroker, transactionalId

  beforeEach(async () => {
    transactionalId = `producer-group-id-${secureRandom()}`

    seedBroker = new Broker({
      connection: createConnection(),
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
      connection: createConnection({ host, port }),
      logger: newLogger(),
    })
    await broker.connect()
  })

  afterEach(async () => {
    await seedBroker.disconnect()
    await broker.disconnect()
  })

  test('request with transaction id', async () => {
    const response = await broker.initProducerId({
      transactionalId,
      transactionTimeout: 30000,
    })

    expect(response).toEqual({
      errorCode: 0,
      throttleTime: 0,
      producerId: expect.stringMatching(/\d+/),
      producerEpoch: expect.any(Number),
    })
  })

  test('rejects transactional id different from coordinator', async () => {
    expect(
      broker.initProducerId({
        transactionalId: transactionalId + 'a',
        transactionTimeout: 30000,
      })
    ).rejects.toEqual(
      new KafkaJSProtocolError('This is not the correct coordinator for this group')
    )
  })

  test('request without transaction id', async () => {
    const response = await broker.initProducerId({ transactionTimeout: 30000 })

    expect(response).toEqual({
      errorCode: 0,
      throttleTime: 0,
      producerId: expect.stringMatching(/\d+/),
      producerEpoch: expect.any(Number),
    })
  })
})
