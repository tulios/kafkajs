const Broker = require('../index')
const { MemberMetadata } = require('../../consumer/assignerProtocol')
const { secureRandom, createConnectionPool, newLogger, retryProtocol } = require('testHelpers')

describe('Broker > JoinGroup', () => {
  let groupId, topicName, seedBroker, broker

  beforeEach(async () => {
    groupId = `consumer-group-id-${secureRandom()}`
    topicName = `test-topic-${secureRandom()}`
    seedBroker = new Broker({
      connectionPool: createConnectionPool(),
      logger: newLogger(),
    })
    await seedBroker.connect()

    const {
      coordinator: { host, port },
    } = await retryProtocol(
      'GROUP_COORDINATOR_NOT_AVAILABLE',
      async () => await seedBroker.findGroupCoordinator({ groupId })
    )

    broker = new Broker({
      connectionPool: createConnectionPool({ host, port }),
      logger: newLogger(),
    })
    await broker.connect()
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
  })

  test('request', async () => {
    const response = await broker.joinGroup({
      groupId,
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      groupProtocols: [
        {
          name: 'AssignerName',
          metadata: MemberMetadata.encode({ version: 1, topics: [topicName] }),
        },
      ],
    })

    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      errorCode: 0,
      generationId: expect.any(Number),
      groupProtocol: 'AssignerName',
      leaderId: expect.any(String),
      memberId: expect.any(String),
      members: expect.arrayContaining([
        expect.objectContaining({
          memberId: expect.any(String),
          groupInstanceId: null,
          memberMetadata: expect.any(Buffer),
        }),
      ]),
    })
  })
})
