const { UserData, DEFAULT_GENERATION } = require('../userData')

describe('Consumer > assigners > cooperativeSticky > UserData', () => {
  test('round-trips ownedPartitions and generationId', () => {
    const ownedPartitions = { 'topic-A': [0, 2, 4], 'topic-B': [1] }
    const decoded = UserData.decode(UserData.encode({ generationId: 7, ownedPartitions }))

    expect(decoded.generationId).toEqual(7)
    expect(decoded.ownedPartitions).toEqual(ownedPartitions)
    expect(decoded.version).toEqual(0)
  })

  test('sorts topics and partitions deterministically', () => {
    const a = UserData.encode({
      generationId: 1,
      ownedPartitions: { 'topic-B': [2, 0], 'topic-A': [3, 1] },
    })
    const b = UserData.encode({
      generationId: 1,
      ownedPartitions: { 'topic-A': [1, 3], 'topic-B': [0, 2] },
    })

    expect(a.equals(b)).toBe(true)
  })

  test('does not mutate the input ownedPartitions arrays', () => {
    const ownedPartitions = { 'topic-A': [4, 2, 0] }
    UserData.encode({ generationId: 1, ownedPartitions })
    expect(ownedPartitions['topic-A']).toEqual([4, 2, 0])
  })

  test('decodes an empty buffer to defaults without throwing', () => {
    expect(UserData.decode(Buffer.alloc(0))).toEqual({
      version: 0,
      generationId: DEFAULT_GENERATION,
      ownedPartitions: {},
    })
  })

  test('decodes a null/undefined buffer to defaults', () => {
    expect(UserData.decode(null)).toEqual({
      version: 0,
      generationId: DEFAULT_GENERATION,
      ownedPartitions: {},
    })
    expect(UserData.decode(undefined)).toEqual({
      version: 0,
      generationId: DEFAULT_GENERATION,
      ownedPartitions: {},
    })
  })

  test('decodes a roundRobin-style (empty bytes) userData to defaults', () => {
    // roundRobin emits no userData; MemberMetadata.encode defaults it to Buffer.alloc(0)
    const roundRobinUserData = Buffer.alloc(0)
    expect(UserData.decode(roundRobinUserData)).toEqual({
      version: 0,
      generationId: DEFAULT_GENERATION,
      ownedPartitions: {},
    })
  })

  test('round-trips with missing/empty fields', () => {
    const decoded = UserData.decode(UserData.encode({}))
    expect(decoded).toEqual({
      version: 0,
      generationId: DEFAULT_GENERATION,
      ownedPartitions: {},
    })
  })

  test('round-trips multi-topic with empty partition lists', () => {
    const ownedPartitions = { 'topic-A': [], 'topic-B': [0, 1, 2] }
    const decoded = UserData.decode(UserData.encode({ generationId: 3, ownedPartitions }))
    expect(decoded.ownedPartitions).toEqual(ownedPartitions)
  })

  test('decodes a short (int16-only) buffer to defaults', () => {
    expect(UserData.decode(Buffer.from([0x00]))).toEqual({
      version: 0,
      generationId: DEFAULT_GENERATION,
      ownedPartitions: {},
    })
  })
})
