const createPartitioner = require('./index')

describe('Producer > Partitioner > Sticky', () => {
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

  afterEach(() => {
    jest.useRealTimers()
  })

  test('same key yields same partition', () => {
    const partitionA = partitioner({ topic, partitionMetadata, message: { key: 'test-key' } })
    const partitionB = partitioner({ topic, partitionMetadata, message: { key: 'test-key' } })
    expect(partitionA).toEqual(partitionB)
  })

  test('sticky with unavailable partitions', () => {
    partitionMetadata[0].leader = -1
    let countForPartition0 = 0
    let countForPartition2 = 0

    for (let i = 1; i <= 100; i++) {
      const partition = partitioner({ topic, partitionMetadata, message: {} })
      expect([0, 2]).toContain(partition)
      partition === 0 ? countForPartition0++ : countForPartition2++
    }

    // The distribution between two available partitions should be even
    expect(countForPartition0 === 100 || countForPartition2 === 100).toBeTrue()
  })

  test('partitions are sticky', () => {
    const partitionCount = {}

    for (let i = 0; i < 30; ++i) {
      const partition = partitioner({ topic, partitionMetadata, message: {} })
      const count = partitionCount[partition] || 0
      partitionCount[partition] = count + 1
    }

    expect(Object.keys(partitionCount).length).toEqual(1)
    expect(Object.values(partitionCount)[0]).toEqual(30)
  })

  test('sticky partition changes between batches', () => {
    jest.useFakeTimers()
    const partitionCount = {}

    for (let i = 0; i < 30; ++i) {
      const partition = partitioner({ topic, partitionMetadata, message: {} })
      const count = partitionCount[partition] || 0
      partitionCount[partition] = count + 1
      if (i % 10 === 0) {
        jest.advanceTimersByTime(1000)
      }
    }

    expect(Object.keys(partitionCount).length).toBeGreaterThan(1)
    expect(Object.values(partitionCount)[0]).toBeLessThan(30)
    expect(Object.values(partitionCount)[1]).toBeLessThan(30)
  })

  test('messages are partitioned in a sticky fashion for each topic', () => {
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

    expect(Object.keys(topics[0].partitionCount).length).toEqual(1)
    expect(Object.values(topics[0].partitionCount)[0]).toEqual(30)
    expect(Object.keys(topics[1].partitionCount).length).toEqual(1)
    expect(Object.values(topics[1].partitionCount)[0]).toEqual(30)
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
