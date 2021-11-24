const concurrencyManager = require('./concurrencyManager')

describe('Consumer > ConcurrencyManager', () => {
  const assignment = [
    { topic: 'topic1', partitions: [1, 2, 3] },
    { topic: 'topic2', partitions: [3, 6, 8] },
  ]

  const findReadReplicaForPartitions = (_, partitions) => ({
    1: partitions.filter(p => p % 3 === 0),
    2: partitions.filter(p => p % 3 === 1),
    3: partitions.filter(p => p % 3 === 2),
  })

  it('should assign all partitions to a single runner', () => {
    const { assign, filterConcurrent } = concurrencyManager({ concurrency: 1 })

    assign({ assignment, findReadReplicaForPartitions })

    expect(filterConcurrent(0)(assignment)).toEqual(assignment)
  })

  it('should distribute partitions by nodes', () => {
    const { assign, filterConcurrent } = concurrencyManager({ concurrency: 3 })

    assign({ assignment, findReadReplicaForPartitions })

    expect(filterConcurrent(1)(assignment)).toEqual([{ topic: 'topic1', partitions: [1] }])
    expect(filterConcurrent(2)(assignment)).toEqual([
      { topic: 'topic1', partitions: [2] },
      { topic: 'topic2', partitions: [8] },
    ])
    expect(filterConcurrent(0)(assignment)).toEqual([
      { topic: 'topic1', partitions: [3] },
      { topic: 'topic2', partitions: [3, 6] },
    ])
  })

  it('should distribute load across remaining runners', () => {
    // TODO
    const { assign, filterConcurrent } = concurrencyManager({ concurrency: 4 })

    assign({ assignment, findReadReplicaForPartitions })

    expect(filterConcurrent(0)(assignment)).toEqual([
      { topic: 'topic1', partitions: [3] },
      { topic: 'topic2', partitions: [3, 6] },
    ])
    expect(filterConcurrent(1)(assignment)).toEqual([{ topic: 'topic1', partitions: [1] }])
    expect(filterConcurrent(2)(assignment)).toEqual([
      { topic: 'topic1', partitions: [2] },
      { topic: 'topic2', partitions: [8] },
    ])
    expect(filterConcurrent(3)(assignment)).toEqual([])
  })
})
