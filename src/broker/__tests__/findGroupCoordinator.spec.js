const Broker = require('../index')
const { secureRandom, createConnectionPool, newLogger, retryProtocol } = require('testHelpers')

describe('Broker > FindGroupCoordinator', () => {
  let groupId, seedBroker

  beforeEach(async () => {
    groupId = `consumer-group-id-${secureRandom()}`
    seedBroker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })
    await seedBroker.connect()
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
  })

  test('request', async () => {
    const response = await retryProtocol(
      'GROUP_COORDINATOR_NOT_AVAILABLE',
      async () => await seedBroker.findGroupCoordinator({ groupId })
    )

    expect(response).toEqual({
      errorCode: 0,
      errorMessage: expect.toBeOneOf([null, 'NONE']),
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      coordinator: {
        nodeId: expect.any(Number),
        host: 'localhost',
        port: expect.any(Number),
      },
    })
  })
})
