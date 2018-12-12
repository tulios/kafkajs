const { createCluster } = require('testHelpers')

describe('Cluster', () => {
  let groupId

  beforeEach(() => {
    groupId = 'test-group-id'
  })

  const testWithOffsets = offsets => {
    it(`should return all committed offsets by group ${
      offsets ? '' : '(no offset map provided)'
    }`, async () => {
      const cluster = createCluster({ offsets })
      const topic = 'test-topic'
      const partition = 0

      expect(cluster.committedOffsets({ groupId })).toEqual({})

      cluster.markOffsetAsCommitted({ groupId, topic, partition, offset: '100' })
      cluster.markOffsetAsCommitted({ groupId: 'foobar', topic, partition, offset: '999' })

      expect(cluster.committedOffsets({ groupId })).toEqual({ [topic]: { [partition]: '100' } })
      expect(cluster.committedOffsets({ groupId: 'foobar' })).toEqual({
        [topic]: { [partition]: '999' },
      })
    })

    if (offsets) {
      it('should use the provided offsets map', async () => {
        const cluster = createCluster({ offsets })
        const topic = 'test-topic'
        const partition = 0

        cluster.markOffsetAsCommitted({ groupId, topic, partition, offset: '100' })
        cluster.markOffsetAsCommitted({ groupId: 'foobar', topic, partition, offset: '999' })

        expect(cluster.committedOffsets({ groupId })).toEqual({ [topic]: { [partition]: '100' } })
        expect(offsets.get(groupId)).toEqual({ [topic]: { [partition]: '100' } })
      })
    }
  }

  testWithOffsets(undefined)
  testWithOffsets(new Map())
})
