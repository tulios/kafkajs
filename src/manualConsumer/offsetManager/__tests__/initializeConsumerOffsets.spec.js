const initializeConsumerOffsets = require('../initializeConsumerOffsets')

describe('Consumer > OffsetMananger > initializeConsumerOffsets', () => {
  it('initialize consumer offsets assigned to -1 with topic offsets', () => {
    const consumerOffsets = [
      {
        topic: 'topic-name1',
        partitions: [
          { partition: 0, offset: '-1', metadata: '', errorCode: 0 },
          { partition: 1, offset: '-1', metadata: '', errorCode: 0 },
          { partition: 2, offset: '14', metadata: '', errorCode: 0 },
          { partition: 3, offset: '-1', metadata: '', errorCode: 0 },
        ],
      },
      {
        topic: 'topic-name2',
        partitions: [
          { partition: 0, offset: '-1', metadata: '', errorCode: 0 },
          { partition: 1, offset: '2', metadata: '', errorCode: 0 },
        ],
      },
    ]
    const topicOffsets = [
      {
        topic: 'topic-name1',
        partitions: [
          { partition: 0, offset: '-1', errorCode: 0 },
          { partition: 1, offset: '3', errorCode: 0 },
          { partition: 2, offset: '16', errorCode: 0 },
          { partition: 3, offset: '8', errorCode: 0 },
        ],
      },
      {
        topic: 'topic-name2',
        partitions: [
          { partition: 0, offset: '1', errorCode: 0 },
          { partition: 1, offset: '2', errorCode: 0 },
        ],
      },
    ]

    expect(initializeConsumerOffsets(consumerOffsets, topicOffsets)).toEqual([
      {
        topic: 'topic-name1',
        partitions: [
          { partition: 0, offset: '-1' },
          { partition: 1, offset: '3' },
          { partition: 2, offset: '14' },
          { partition: 3, offset: '8' },
        ],
      },
      {
        topic: 'topic-name2',
        partitions: [
          { partition: 0, offset: '1' },
          { partition: 1, offset: '2' },
        ],
      },
    ])
  })
})
