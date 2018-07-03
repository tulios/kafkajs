const groupMessagesPerPartition = require('./groupMessagesPerPartition')
const { createModPartitioner } = require('testHelpers')

describe('Producer > groupMessagesPerPartition', () => {
  let topic, partitionMetadata, messages, partitioner

  beforeEach(() => {
    topic = 'test-topic'
    partitionMetadata = [
      { partitionId: 1, leader: 1 },
      { partitionId: 2, leader: 2 },
      { partitionId: 0, leader: 0 },
    ]

    messages = [
      { key: '1' },
      { key: '2' },
      { key: '3' },
      { key: '4' },
      { key: '5' },
      { key: '6' },
      { key: '7' },
      { key: '8' },
      { key: '9' },
    ]
    partitioner = createModPartitioner()
  })

  test('group messages per partition', () => {
    const result = groupMessagesPerPartition({ topic, partitionMetadata, messages, partitioner })
    expect(result).toEqual({
      '0': [{ key: '3' }, { key: '6' }, { key: '9' }],
      '1': [{ key: '1' }, { key: '4' }, { key: '7' }],
      '2': [{ key: '2' }, { key: '5' }, { key: '8' }],
    })
  })

  test('returns empty when called with no partition metadata', () => {
    expect(
      groupMessagesPerPartition({ topic, partitionMetadata: [], messages, partitioner })
    ).toEqual({})
  })
})
