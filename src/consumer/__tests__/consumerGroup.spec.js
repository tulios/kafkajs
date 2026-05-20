const ConsumerGroup = require('../consumerGroup')
const { MemberAssignment, MemberMetadata } = require('../assignerProtocol')
const { newLogger } = require('testHelpers')

describe('ConsumerGroup', () => {
  let consumerGroup

  beforeEach(() => {
    consumerGroup = new ConsumerGroup({
      logger: newLogger(),
      topics: ['topic1'],
      cluster: {},
    })
  })

  describe('uncommittedOffsets', () => {
    it("calls the offset manager's uncommittedOffsets", async () => {
      const mockOffsets = { topics: [] }
      consumerGroup.offsetManager = { uncommittedOffsets: jest.fn(() => mockOffsets) }

      expect(consumerGroup.uncommittedOffsets()).toStrictEqual(mockOffsets)
      expect(consumerGroup.offsetManager.uncommittedOffsets).toHaveBeenCalled()
    })
  })

  describe('commitOffsets', () => {
    it("calls the offset manager's commitOffsets", async () => {
      consumerGroup.offsetManager = { commitOffsets: jest.fn(() => Promise.resolve()) }

      const offsets = { topics: [{ partitions: [{ offset: '0', partition: 0 }] }] }
      await consumerGroup.commitOffsets(offsets)
      expect(consumerGroup.offsetManager.commitOffsets).toHaveBeenCalledTimes(1)
      expect(consumerGroup.offsetManager.commitOffsets).toHaveBeenCalledWith(offsets)
    })
  })

  describe('mixed subscriptions and cooperative flow', () => {
    const getPrivateSymbol = (instance, name) =>
      Object.getOwnPropertySymbols(Object.getPrototypeOf(instance)).find(s =>
        s.toString().includes(name)
      )

    test('leader loads union of member topics before assignment', async () => {
      const cluster = {
        addMultipleTargetTopics: jest.fn(async () => {}),
        refreshMetadata: jest.fn(async () => {}),
        findTopicPartitionMetadata: jest.fn(topic => [{ partitionId: 0, topic }]),
        committedOffsets: jest.fn(() => ({})),
      }
      const assigner = {
        name: 'roundRobin',
        assign: jest.fn(async () => []),
      }

      consumerGroup = new ConsumerGroup({
        logger: newLogger(),
        topics: ['topic1'],
        cluster,
        groupId: 'group-1',
        topicConfigurations: {},
        instrumentationEmitter: { emit: jest.fn() },
        assigners: [assigner],
      })

      consumerGroup.coordinator = {
        syncGroup: jest.fn(async () => ({
          memberAssignment: MemberAssignment.encode({
            version: 0,
            assignment: { topic1: [0], topic2: [0] },
          }),
        })),
      }

      consumerGroup.generationId = 1
      consumerGroup.groupProtocol = 'roundRobin'
      consumerGroup.memberId = 'leader'
      consumerGroup.leaderId = 'leader'
      consumerGroup.members = [
        {
          memberId: 'leader',
          memberMetadata: MemberMetadata.encode({ version: 0, topics: ['topic1'] }),
        },
        {
          memberId: 'follower',
          memberMetadata: MemberMetadata.encode({ version: 0, topics: ['topic1', 'topic2'] }),
        },
      ]

      const syncSymbol = getPrivateSymbol(consumerGroup, 'private:ConsumerGroup:sync')
      await consumerGroup[syncSymbol]()

      expect(cluster.addMultipleTargetTopics).toHaveBeenCalledTimes(1)
      const [topicsArg] = cluster.addMultipleTargetTopics.mock.calls[0]
      expect(topicsArg.sort()).toEqual(['topic1', 'topic2'])
      expect(assigner.assign).toHaveBeenCalledWith({
        members: consumerGroup.members,
        topics: ['topic1'],
        allSubscribedTopics: ['topic1', 'topic2'],
      })
    })

    test('joinAndSync loops for cooperative second round when requested by assigner', async () => {
      consumerGroup = new ConsumerGroup({
        logger: newLogger(),
        topics: ['topic1'],
        cluster: {},
        groupId: 'group-1',
        topicConfigurations: {},
        instrumentationEmitter: { emit: jest.fn() },
        assigners: [],
      })

      consumerGroup.subscriptionState = {
        assigned: () => [],
      }

      const joinSymbol = getPrivateSymbol(consumerGroup, 'private:ConsumerGroup:join')
      const syncSymbol = getPrivateSymbol(consumerGroup, 'private:ConsumerGroup:sync')

      let syncCalls = 0
      consumerGroup[joinSymbol] = jest.fn(async () => {
        consumerGroup.memberId = 'member-1'
        consumerGroup.leaderId = 'member-1'
        consumerGroup.groupProtocol = 'cooperative-sticky'
      })
      consumerGroup[syncSymbol] = jest.fn(async () => {
        syncCalls += 1
        consumerGroup.needsCooperativeRejoin = syncCalls === 1
        consumerGroup.cooperativeRejoinRequestedBy = syncCalls === 1 ? ['cooperative-sticky'] : []
      })

      consumerGroup.logger = {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
      }

      await consumerGroup.joinAndSync()

      expect(consumerGroup[joinSymbol]).toHaveBeenCalledTimes(2)
      expect(consumerGroup[syncSymbol]).toHaveBeenCalledTimes(2)
      expect(consumerGroup.logger.warn).toHaveBeenCalledWith(
        'Cooperative assigner requested additional join/sync round',
        expect.objectContaining({
          cooperativeRejoinRound: 1,
          requestedBy: ['cooperative-sticky'],
        })
      )
    })

    test('joinAndSync fails fast after max cooperative rounds to avoid silent infinite loops', async () => {
      consumerGroup = new ConsumerGroup({
        logger: newLogger(),
        topics: ['topic1'],
        cluster: {},
        groupId: 'group-1',
        topicConfigurations: {},
        instrumentationEmitter: { emit: jest.fn() },
        assigners: [],
      })

      consumerGroup.subscriptionState = {
        assigned: () => [],
      }

      consumerGroup.logger = {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
      }

      const joinSymbol = getPrivateSymbol(consumerGroup, 'private:ConsumerGroup:join')
      const syncSymbol = getPrivateSymbol(consumerGroup, 'private:ConsumerGroup:sync')

      consumerGroup[joinSymbol] = jest.fn(async () => {
        consumerGroup.memberId = 'member-1'
        consumerGroup.leaderId = 'member-1'
        consumerGroup.groupProtocol = 'cooperative-sticky'
      })
      consumerGroup[syncSymbol] = jest.fn(async () => {
        consumerGroup.needsCooperativeRejoin = true
        consumerGroup.cooperativeRejoinRequestedBy = ['cooperative-sticky']
      })

      await expect(consumerGroup.joinAndSync()).rejects.toThrow(
        'Exceeded maximum cooperative rejoin rounds (20)'
      )
      expect(consumerGroup[joinSymbol]).toHaveBeenCalledTimes(20)
      expect(consumerGroup[syncSymbol]).toHaveBeenCalledTimes(20)
      expect(consumerGroup.logger.error).toHaveBeenCalledWith(
        'Exceeded cooperative rejoin round limit',
        expect.objectContaining({
          maxCooperativeRejoinRounds: 20,
          requestedBy: ['cooperative-sticky'],
        })
      )
    })

    test('joinAndSync honors configured max cooperative rounds', async () => {
      consumerGroup = new ConsumerGroup({
        logger: newLogger(),
        topics: ['topic1'],
        cluster: {},
        groupId: 'group-1',
        topicConfigurations: {},
        instrumentationEmitter: { emit: jest.fn() },
        assigners: [],
        maxCooperativeRejoinRounds: 2,
      })

      consumerGroup.subscriptionState = {
        assigned: () => [],
      }

      consumerGroup.logger = {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
      }

      const joinSymbol = getPrivateSymbol(consumerGroup, 'private:ConsumerGroup:join')
      const syncSymbol = getPrivateSymbol(consumerGroup, 'private:ConsumerGroup:sync')

      consumerGroup[joinSymbol] = jest.fn(async () => {
        consumerGroup.memberId = 'member-1'
        consumerGroup.leaderId = 'member-1'
        consumerGroup.groupProtocol = 'cooperative-sticky-kafkajs'
      })
      consumerGroup[syncSymbol] = jest.fn(async () => {
        consumerGroup.needsCooperativeRejoin = true
        consumerGroup.cooperativeRejoinRequestedBy = ['cooperative-sticky-kafkajs']
      })

      await expect(consumerGroup.joinAndSync()).rejects.toThrow(
        'Exceeded maximum cooperative rejoin rounds (2)'
      )
      expect(consumerGroup[joinSymbol]).toHaveBeenCalledTimes(2)
      expect(consumerGroup[syncSymbol]).toHaveBeenCalledTimes(2)
      expect(consumerGroup.logger.error).toHaveBeenCalledWith(
        'Exceeded cooperative rejoin round limit',
        expect.objectContaining({
          maxCooperativeRejoinRounds: 2,
        })
      )
    })
  })
})
