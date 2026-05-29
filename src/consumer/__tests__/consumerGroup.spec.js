const ConsumerGroup = require('../consumerGroup')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { MemberAssignment } = require('../assignerProtocol')
const { roundRobin, cooperativeSticky } = require('../assigners')
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

  describe('mixed-protocol state isolation', () => {
    const topic = 'topic1'
    const partitions = [0, 1]

    const buildConsumerGroup = ({ groupProtocol }) => {
      const cluster = {
        findGroupCoordinator: jest.fn(async () => coordinator),
        refreshMetadata: jest.fn(async () => {}),
        refreshMetadataIfNecessary: jest.fn(async () => {}),
        findTopicPartitionMetadata: jest.fn(() => partitions.map(partitionId => ({ partitionId }))),
        committedOffsets: jest.fn(() => ({})),
      }

      const coordinator = {
        joinGroup: jest.fn(async () => ({
          generationId: 1,
          leaderId: 'leader-member',
          memberId: 'this-member',
          members: [],
          groupProtocol,
        })),
        syncGroup: jest.fn(async () => ({
          memberAssignment: MemberAssignment.encode({
            version: 0,
            assignment: { [topic]: partitions },
          }),
        })),
        isConnected: () => true,
      }

      return new ConsumerGroup({
        logger: newLogger(),
        topics: [topic],
        topicConfigurations: { [topic]: {} },
        cluster,
        groupId: 'group-id',
        instrumentationEmitter: new InstrumentationEventEmitter(),
        assigners: [cooperativeSticky({ cluster }), roundRobin({ cluster })],
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        autoCommit: true,
        autoCommitInterval: null,
        autoCommitThreshold: null,
      })
    }

    it('records no cooperative state when an unselected cooperative assigner is configured', async () => {
      const group = buildConsumerGroup({ groupProtocol: 'RoundRobinAssigner' })

      await group.joinAndSync()

      expect(group.isCooperative()).toEqual(false)
      expect(group.ownedGenerationId).toEqual(-1)
      expect(group.rejoinNeeded).toEqual(false)
      expect(group.cooperativeRounds).toEqual(0)
      expect(group.lastRevoked).toEqual([])
      expect(group.lastAdded).toEqual([])
    })

    it('records cooperative state when the cooperative assigner is selected', async () => {
      const group = buildConsumerGroup({ groupProtocol: 'cooperative-sticky' })

      await group.joinAndSync()

      expect(group.isCooperative()).toEqual(true)
      expect(group.ownedGenerationId).toEqual(1)
      expect(group.rejoinNeeded).toEqual(false)
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

  describe('maxCooperativeRejoinRounds', () => {
    const topic = 'topic1'

    const buildCooperativeGroup = ({ maxCooperativeRejoinRounds } = {}) => {
      const group = new ConsumerGroup({
        logger: newLogger(),
        topics: [topic],
        cluster: {},
        maxCooperativeRejoinRounds,
      })

      group.offsetManager = {
        generationId: 0,
        memberId: 'member',
        coordinator: {},
        uncommittedOffsets: jest.fn(() => ({ topics: [] })),
        updateAssignment: jest.fn(),
        commitOffsets: jest.fn(() => Promise.resolve()),
      }

      return group
    }

    // Drive one cooperative SYNC round: the group currently owns `owned`, the
    // leader now assigns `assigned`; anything in owned but not assigned is
    // revoked, which is what arms the rejoin loop.
    const runRound = async (group, { owned, assigned }) => {
      group.subscriptionState.assign([{ topic, partitions: owned }])
      const currentMemberAssignment = [{ topic, partitions: assigned }]
      await group.applyCooperativeAssignment({
        currentMemberAssignment,
        newAssignmentByTopic: { [topic]: assigned },
        generationId: 1,
        memberId: 'member',
        coordinator: {},
      })
    }

    it('defaults to 2 rounds before degrading', async () => {
      const group = buildCooperativeGroup()

      await runRound(group, { owned: [0, 1], assigned: [0] })
      expect(group.rejoinNeeded).toEqual(true)
      expect(group.cooperativeRounds).toEqual(1)

      await runRound(group, { owned: [0], assigned: [] })
      expect(group.rejoinNeeded).toEqual(true)
      expect(group.cooperativeRounds).toEqual(2)
      expect(group.offsetManager.commitOffsets).not.toHaveBeenCalled()
    })

    it('honors a custom cap, degrading at the configured threshold', async () => {
      const group = buildCooperativeGroup({ maxCooperativeRejoinRounds: 1 })

      await runRound(group, { owned: [0, 1], assigned: [0] })
      expect(group.rejoinNeeded).toEqual(true)
      expect(group.cooperativeRounds).toEqual(1)

      await runRound(group, { owned: [0], assigned: [] })
      expect(group.rejoinNeeded).toEqual(false)
      expect(group.cooperativeRounds).toEqual(0)
      expect(group.offsetManager.commitOffsets).toHaveBeenCalledTimes(1)
    })
  })
})
