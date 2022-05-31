const createTopicData = require('./createTopicData')

describe('Producer > createTopicData', () => {
  let topic, partitions, messagesPerPartition

  beforeEach(() => {
    topic = 'test-topic'
    partitions = [1, 2, 3]

    messagesPerPartition = {
      1: [{ key: '1' }],
      2: [{ key: '2' }],
      3: [{ key: '3' }, { key: '4' }],
    }
  })

  test('format data by topic and partition', () => {
    const result = createTopicData([{ topic, partitions, messagesPerPartition }])
    expect(result).toEqual([
      {
        topic,
        partitions: [
          { partition: 1, messages: [{ key: '1' }] },
          { partition: 2, messages: [{ key: '2' }] },
          { partition: 3, messages: [{ key: '3' }, { key: '4' }] },
        ],
      },
    ])
  })
})
