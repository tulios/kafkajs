const createTopicData = require('./createTopicData')

describe('Producer > createTopicData', () => {
  let topic, partitions, messagesPerPartition, sequencePerPartition

  beforeEach(() => {
    topic = 'test-topic'
    partitions = [1, 2, 3]

    messagesPerPartition = {
      1: [{ key: '1' }],
      2: [{ key: '2' }],
      3: [{ key: '3' }, { key: '4' }],
    }

    sequencePerPartition = {
      1: 0,
      2: 5,
      3: 10,
    }
  })

  test('format data by topic and partition', () => {
    const result = createTopicData([
      { topic, partitions, messagesPerPartition, sequencePerPartition },
    ])
    expect(result).toEqual([
      {
        topic,
        partitions: [
          { partition: 1, firstSequence: 0, messages: [{ key: '1' }] },
          { partition: 2, firstSequence: 5, messages: [{ key: '2' }] },
          { partition: 3, firstSequence: 10, messages: [{ key: '3' }, { key: '4' }] },
        ],
      },
    ])
  })
})
