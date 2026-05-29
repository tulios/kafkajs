const OffsetManager = require('../index')

describe('Consumer > OffsetManager > updateAssignment (cooperative)', () => {
  let offsetManager, topic1, topic2, memberAssignment

  beforeEach(() => {
    topic1 = 'topic-1'
    topic2 = 'topic-2'

    memberAssignment = {
      [topic1]: [0, 1],
      [topic2]: [0, 1],
    }

    offsetManager = new OffsetManager({ memberAssignment })
  })

  it('preserves resolved offsets for retained partitions (F1 guard)', () => {
    // Resolve an offset on a partition we will retain.
    offsetManager.resolveOffset({ topic: topic1, partition: 0, offset: '41' })
    expect(offsetManager.resolvedOffsets[topic1][0]).toEqual('42')

    // Revoke topic1/1 and topic2/*, add a brand new partition; retain topic1/0.
    offsetManager.updateAssignment({
      added: [{ topic: topic1, partition: 2 }],
      revoked: [
        { topic: topic1, partition: 1 },
        { topic: topic2, partition: 0 },
        { topic: topic2, partition: 1 },
      ],
    })

    // Retained partition keeps its resolved offset.
    expect(offsetManager.resolvedOffsets[topic1][0]).toEqual('42')
  })

  it('indexes newly added partitions', () => {
    offsetManager.updateAssignment({ added: [{ topic: 'topic-3', partition: 0 }], revoked: [] })

    expect(offsetManager.memberAssignment['topic-3']).toEqual([0])
    expect(offsetManager.resolvedOffsets['topic-3']).toEqual({})
    expect(offsetManager.topics).toContain('topic-3')
  })

  it('drops revoked partitions from assignment and resolved offsets', () => {
    offsetManager.resolveOffset({ topic: topic1, partition: 1, offset: '10' })

    offsetManager.updateAssignment({ added: [], revoked: [{ topic: topic1, partition: 1 }] })

    expect(offsetManager.memberAssignment[topic1]).toEqual([0])
    expect(offsetManager.resolvedOffsets[topic1][1]).toBeUndefined()
  })

  it('removes a topic entirely when all its partitions are revoked', () => {
    offsetManager.updateAssignment({
      added: [],
      revoked: [
        { topic: topic2, partition: 0 },
        { topic: topic2, partition: 1 },
      ],
    })

    expect(offsetManager.memberAssignment[topic2]).toBeUndefined()
    expect(offsetManager.resolvedOffsets[topic2]).toBeUndefined()
    expect(offsetManager.topics).not.toContain(topic2)
  })

  it('does not duplicate an added partition that is already assigned', () => {
    offsetManager.updateAssignment({ added: [{ topic: topic1, partition: 0 }], revoked: [] })
    expect(offsetManager.memberAssignment[topic1]).toEqual([0, 1])
  })

  it('handles simultaneous add and revoke, keeping retained untouched', () => {
    offsetManager.resolveOffset({ topic: topic1, partition: 0, offset: '99' })

    offsetManager.updateAssignment({
      added: [{ topic: topic2, partition: 2 }],
      revoked: [{ topic: topic1, partition: 1 }],
    })

    expect(offsetManager.resolvedOffsets[topic1][0]).toEqual('100')
    expect(offsetManager.memberAssignment[topic1]).toEqual([0])
    expect(offsetManager.memberAssignment[topic2]).toEqual([0, 1, 2])
  })
})
