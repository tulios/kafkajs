const OffsetManager = require('../index')

describe('Consumer > OffsetMananger > countResolvedOffsets', () => {
  let offsetManager
  const resolveOffsets = (topic, partition, { count, startFrom = 0 }) => {
    Array(count)
      .fill()
      .forEach((_, i) =>
        offsetManager.resolveOffset({
          topic,
          partition,
          offset: (startFrom + i).toString(),
        })
      )
  }

  beforeEach(() => {
    const memberAssignment = {
      topic1: [0, 1, 2, 3],
      topic2: [0, 1, 2, 3, 4, 5],
    }

    offsetManager = new OffsetManager({
      memberAssignment,
      cluster: {
        committedOffsets: jest.fn(() => new Map()),
      },
    })
  })

  it('counts the number of resolved offsets for all topics', () => {
    offsetManager.committedOffsets()['topic1'][0] = '-1'
    offsetManager.committedOffsets()['topic1'][1] = '-1'
    offsetManager.committedOffsets()['topic1'][2] = '-1'
    offsetManager.committedOffsets()['topic2'][5] = '-1'

    resolveOffsets('topic1', 0, { count: 10 })
    resolveOffsets('topic1', 1, { count: 1 })
    resolveOffsets('topic1', 2, { count: 3 })
    resolveOffsets('topic2', 5, { count: 6 })

    expect(offsetManager.countResolvedOffsets().toString()).toEqual('20')
  })

  it('takes the committed offsets in consideration', () => {
    // committedOffsets will always have the next offset or -1
    offsetManager.committedOffsets()['topic1'][0] = '10'
    offsetManager.committedOffsets()['topic1'][1] = '1'
    offsetManager.committedOffsets()['topic1'][2] = '3'
    offsetManager.committedOffsets()['topic2'][5] = '5' // missing 1

    resolveOffsets('topic1', 0, { count: 10 })
    resolveOffsets('topic1', 1, { count: 1 })
    resolveOffsets('topic1', 2, { count: 3 })
    resolveOffsets('topic2', 5, { count: 6 })

    expect(offsetManager.countResolvedOffsets().toString()).toEqual('1')
  })
})
