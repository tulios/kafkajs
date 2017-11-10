const Broker = require('./index')
const { secureRandom, createConnection, newLogger, retryProtocol } = require('testHelpers')

describe('Broker > JoinGroup', () => {
  let groupId, seedBroker, broker

  beforeEach(async () => {
    groupId = `consumer-group-id-${secureRandom()}`
    seedBroker = new Broker(createConnection(), newLogger())
    await seedBroker.connect()

    const { coordinator: { host, port } } = await retryProtocol(
      'GROUP_COORDINATOR_NOT_AVAILABLE',
      async () => await seedBroker.findGroupCoordinator({ groupId })
    )

    broker = new Broker(createConnection({ host, port }), newLogger())
    await broker.connect()
  })

  afterEach(async () => {
    await seedBroker.disconnect()
    await broker.disconnect()
  })

  test('request', async () => {
    const response = await broker.joinGroup({
      groupId,
      sessionTimeout: 30000,
    })

    expect(response).toEqual({
      errorCode: 0,
      generationId: expect.any(Number),
      groupProtocol: 'default',
      leaderId: expect.any(String),
      memberId: expect.any(String),
      members: expect.arrayContaining([
        expect.objectContaining({
          memberId: expect.any(String),
          memberMetadata: expect.any(Buffer),
        }),
      ]),
    })
  })
})
