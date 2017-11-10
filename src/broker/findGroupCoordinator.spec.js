const Broker = require('./index')
const { secureRandom, createConnection, newLogger, retryProtocol } = require('testHelpers')

describe('Broker > FindGroupCoordinator', () => {
  let groupId, seedBroker

  beforeEach(async () => {
    groupId = `consumer-group-id-${secureRandom()}`
    seedBroker = new Broker(createConnection(), newLogger())
    await seedBroker.connect()
  })

  afterEach(async () => {
    await seedBroker.disconnect()
  })

  test('request', async () => {
    const response = await retryProtocol(
      'GROUP_COORDINATOR_NOT_AVAILABLE',
      async () => await seedBroker.findGroupCoordinator({ groupId })
    )

    expect(response).toEqual({
      errorCode: 0,
      coordinator: {
        nodeId: expect.any(Number),
        host: expect.stringMatching(/\b((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\.|$)){4}\b/),
        port: expect.any(Number),
      },
    })
  })
})
