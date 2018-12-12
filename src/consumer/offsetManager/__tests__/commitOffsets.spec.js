const OffsetManager = require('../index')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')

describe('Consumer > OffsetMananger > commitOffsets', () => {
  let offsetManager,
    topic1,
    topic2,
    memberAssignment,
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

    mockCoordinator = {
      offsetCommit: jest.fn(),
    }

    offsetManager = new OffsetManager({
      cluster: {
        committedOffsets: jest.fn(() => ({})),
      },
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
})
