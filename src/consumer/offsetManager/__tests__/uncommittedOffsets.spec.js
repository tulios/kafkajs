const OffsetManager = require('../index')

describe('Consumer > OffsetMananger > uncommittedOffsets', () => {
  let offsetManager, topic1, topic2, memberAssignment

  beforeEach(() => {
    topic1 = 'topic-1'
    topic2 = 'topic-2'

    memberAssignment = {
      [topic1]: [0, 1],
      [topic2]: [0, 1, 2, 3],
    }

    offsetManager = new OffsetManager({ memberAssignment })
  })

  it('returns all resolved offsets which have not been committed', () => {
    const defaultOffsetInt = 2

    Object.keys(memberAssignment).forEach(topic => {
      memberAssignment[topic].forEach(partition => {
        offsetManager.resolveOffset({ topic, partition, offset: defaultOffsetInt.toString() })
      })
    })

    const defaultResolvedOffsetInt = defaultOffsetInt + 1

    // If committed offset equal to resolved offset, then partition is marked as committed
    offsetManager.committedOffsets()[topic2][0] = defaultResolvedOffsetInt.toString() // "committed"
    offsetManager.committedOffsets()[topic2][1] = (defaultResolvedOffsetInt - 1).toString() // not "committed"
    offsetManager.committedOffsets()[topic2][2] = (defaultResolvedOffsetInt + 1).toString() // not "committed"

    expect(offsetManager.uncommittedOffsets()).toEqual({
      topics: [
        {
          topic: topic1,
          partitions: [
            { partition: '0', offset: defaultResolvedOffsetInt.toString() },
            { partition: '1', offset: defaultResolvedOffsetInt.toString() },
          ],
        },
        {
          topic: topic2,
          partitions: [
            { partition: '1', offset: defaultResolvedOffsetInt.toString() },
            { partition: '2', offset: defaultResolvedOffsetInt.toString() },
            { partition: '3', offset: defaultResolvedOffsetInt.toString() },
          ],
        },
      ],
    })
  })
})
