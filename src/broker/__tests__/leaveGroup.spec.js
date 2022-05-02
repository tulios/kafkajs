const Broker = require('../index')
const { MemberMetadata, MemberAssignment } = require('../../consumer/assignerProtocol')
const { secureRandom, createConnectionPool, newLogger, retryProtocol } = require('testHelpers')

describe('Broker > LeaveGroup', () => {
  let topicName, groupId, seedBroker, groupCoordinator

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

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

    groupCoordinator = new Broker({
      connectionPool: createConnectionPool({ host, port }),
      logger: newLogger(),
    })
    await groupCoordinator.connect()
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    groupCoordinator && (await groupCoordinator.disconnect())
  })

  test('request', async () => {
    const { generationId, memberId } = await groupCoordinator.joinGroup({
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

    const memberAssignment = MemberAssignment.encode({
      version: 1,
      assignment: { [topicName]: [0] },
    })

    const groupAssignment = [{ memberId, memberAssignment }]
    await groupCoordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupAssignment,
    })

    const response = await groupCoordinator.leaveGroup({ groupId, memberId })
    expect(response).toEqual({
      clientSideThrottleTime: expect.optional(0),
      throttleTime: 0,
      errorCode: 0,
      members: [{ errorCode: 0, memberId, groupInstanceId: null }],
    })
  })
})
