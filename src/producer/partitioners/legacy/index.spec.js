const createPartitioner = require('./index')

describe('Producer > Partitioner > Legacy', () => {
  let topic, partitioner, partitionMetadata

  beforeEach(() => {
    topic = 'test-topic-1'
    partitioner = createPartitioner()

    // Intentionally make the partition list not in partition order
    // to test the edge cases
    partitionMetadata = [
      { partitionId: 1, leader: 1 },
      { partitionId: 2, leader: 2 },
      { partitionId: 0, leader: 0 },
    ]
  })

  test('same key yields same partition', () => {
    const partitionA = partitioner({ topic, partitionMetadata, message: { key: 'test-key' } })
    const partitionB = partitioner({ topic, partitionMetadata, message: { key: 'test-key' } })
    expect(partitionA).toEqual(partitionB)
  })

  test('round-robin with unavailable partitions', () => {
    partitionMetadata[0].leader = -1
    let countForPartition0 = 0
    let countForPartition2 = 0

    for (let i = 1; i <= 100; i++) {
      const partition = partitioner({ topic, partitionMetadata, message: {} })
      expect([0, 2]).toContain(partition)
      partition === 0 ? countForPartition0++ : countForPartition2++
    }

    // The distribution between two available partitions should be even
    expect(countForPartition0).toEqual(countForPartition2)
  })

  test('round-robin', () => {
    const partitionCount = {}

    for (let i = 0; i < 30; ++i) {
      const partition = partitioner({ topic, partitionMetadata, message: {} })
      const count = partitionCount[partition] || 0
      partitionCount[partition] = count + 1
    }

    expect(partitionCount[0]).toEqual(10)
    expect(partitionCount[1]).toEqual(10)
    expect(partitionCount[2]).toEqual(10)
  })

  test('messages are partitioned in a round-robin fashion for each topic', () => {
    const partitionMetadata = [
      { partitionId: 1, leader: 1 },
      { partitionId: 2, leader: 2 },
    ]
    const topics = [
      { topic: 'topic-a', partitionMetadata, partitionCount: {} },
      { topic: 'topic-b', partitionMetadata, partitionCount: {} },
    ]

    for (let i = 0; i < 30; ++i) {
      for (const { topic, partitionMetadata, partitionCount } of topics) {
        const partition = partitioner({ topic, partitionMetadata, message: {} })
        const count = partitionCount[partition] || 0
        partitionCount[partition] = count + 1
      }
    }

    expect(topics[0].partitionCount[1]).toEqual(15)
    expect(topics[0].partitionCount[2]).toEqual(15)
    expect(topics[1].partitionCount[1]).toEqual(15)
    expect(topics[1].partitionCount[2]).toEqual(15)
  })

  test('returns the configured partition if it exists', () => {
    const partition = partitioner({
      topic,
      partitionMetadata,
      message: { key: '1', partition: 99 },
    })

    expect(partition).toEqual(99)
  })

  test('returns the configured partition even if the partition is falsy', () => {
    const partition = partitioner({
      topic,
      partitionMetadata,
      message: { key: '1', partition: 0 },
    })

    expect(partition).toEqual(0)
  })
})
