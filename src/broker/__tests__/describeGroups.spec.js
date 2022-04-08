const Broker = require('../index')
const {
  secureRandom,
  createConnectionPool,
  newLogger,
  retryProtocol,
  createCluster,
} = require('testHelpers')
const createConsumer = require('../../consumer')

describe('Broker > DescribeGroups', () => {
  let groupId, topicName, seedBroker, broker, cluster, consumer

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

    cluster = createCluster()
    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
  })

  test('request', async () => {
    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })
    await consumer.run({ eachMessage: jest.fn() })
    const response = await broker.describeGroups({ groupIds: [groupId] })

    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      groups: [
        {
          errorCode: 0,
          groupId,
          members: [
            {
              clientHost: expect.any(String),
              clientId: expect.any(String),
              memberAssignment: expect.anything(),
              memberId: expect.any(String),
              memberMetadata: expect.anything(),
            },
          ],
          protocol: 'RoundRobinAssigner',
          protocolType: 'consumer',
          state: 'Stable',
        },
      ],
    })
  })
})
