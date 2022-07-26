const OffsetManager = require('../index')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { createErrorFromCode } = require('../../../protocol/error')
const NOT_COORDINATOR_FOR_GROUP_CODE = 16

describe('Consumer > OffsetMananger > commitOffsets', () => {
  let offsetManager,
    topic1,
    topic2,
    memberAssignment,
    mockCluster,
    mockCoordinator,
    groupId,
    generationId,
    memberId

  beforeEach(() => {
    topic1 = 'topic-1'
    topic2 = 'topic-2'
    groupId = 'groupId'
    generationId = 'generationId'
    memberId = 'memberId'

    memberAssignment = {
      [topic1]: [0, 1],
      [topic2]: [0, 1, 2, 3],
    }

    mockCluster = {
      committedOffsets: jest.fn(() => ({})),
      refreshMetadata: jest.fn(() => ({})),
    }

    mockCoordinator = {
      offsetCommit: jest.fn(),
    }

    offsetManager = new OffsetManager({
      cluster: mockCluster,
      memberAssignment,
      groupId,
      generationId,
      memberId,
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })

    offsetManager.getCoordinator = jest.fn(() => mockCoordinator)
  })

  it('commits all the resolved offsets that have not already been committed', async () => {
    const defaultOffsetInt = 2

    Object.keys(memberAssignment).forEach(topic => {
      memberAssignment[topic].forEach(partition => {
        offsetManager.resolveOffset({ topic, partition, offset: defaultOffsetInt.toString() })
      })
    })

    const defaultResolvedOffsetInt = defaultOffsetInt + 1
    const defaultResolvedOffsetStr = defaultResolvedOffsetInt.toString()

    // If committed offset equal to resolved offset, then partition is marked as committed
    offsetManager.committedOffsets()[topic2][0] = defaultResolvedOffsetInt.toString() // "committed"

    await offsetManager.commitOffsets()

    expect(mockCoordinator.offsetCommit).toHaveBeenCalledWith({
      groupId,
      memberId,
      groupGenerationId: generationId,
      topics: [
        {
          topic: topic1,
          partitions: [
            { partition: '0', offset: defaultResolvedOffsetStr },
            { partition: '1', offset: defaultResolvedOffsetStr },
          ],
        },
        {
          topic: topic2,
          partitions: [
            { partition: '1', offset: defaultResolvedOffsetStr },
            { partition: '2', offset: defaultResolvedOffsetStr },
            { partition: '3', offset: defaultResolvedOffsetStr },
          ],
        },
      ],
    })

    expect(offsetManager.committedOffsets()).toEqual({
      'topic-1': {
        '0': defaultResolvedOffsetStr,
        '1': defaultResolvedOffsetStr,
      },
      'topic-2': {
        '0': defaultResolvedOffsetStr,
        '1': defaultResolvedOffsetStr,
        '2': defaultResolvedOffsetStr,
        '3': defaultResolvedOffsetStr,
      },
    })
  })

  it('commits any provided offsets', async () => {
    const offset = Math.random().toString()
    const offsets = { topics: [{ topic: topic1, partitions: [{ partition: '0', offset }] }] }
    await offsetManager.commitOffsets(offsets)

    expect(mockCoordinator.offsetCommit).toHaveBeenCalledWith({
      groupId,
      memberId,
      groupGenerationId: generationId,
      topics: [
        {
          topic: topic1,
          partitions: [{ partition: '0', offset }],
        },
      ],
    })
  })

  it('refreshes metadata on NOT_COORDINATOR_FOR_GROUP protocol error', async () => {
    mockCoordinator.offsetCommit.mockImplementation(() => {
      throw createErrorFromCode(NOT_COORDINATOR_FOR_GROUP_CODE)
    })

    const offset = Math.random().toString()
    const offsets = { topics: [{ topic: topic1, partitions: [{ partition: '0', offset }] }] }

    await expect(offsetManager.commitOffsets(offsets)).rejects.toThrow()
    expect(mockCluster.refreshMetadata).toHaveBeenCalled()
  })
})
