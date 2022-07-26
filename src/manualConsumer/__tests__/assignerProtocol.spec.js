const { MemberMetadata, MemberAssignment } = require('../assignerProtocol')

const FIXTURE_ROUND_ROBIN_METADATA = Buffer.from(
  require('./fixtures/roundRobinAssigner/memberMetadata.json')
)
const FIXTURE_ROUND_ROBIN_ASSIGNER = Buffer.from(
  require('./fixtures/roundRobinAssigner/memberAssignment.json')
)

describe('Consumer > assignerProtocol', () => {
  describe('MemberMetadata', () => {
    test('encode', () => {
      const buffer = MemberMetadata.encode({
        version: 1,
        topics: ['topic-test'],
      })

      expect(buffer).toEqual(FIXTURE_ROUND_ROBIN_METADATA)
    })

    test('decode', () => {
      expect(MemberMetadata.decode(FIXTURE_ROUND_ROBIN_METADATA)).toEqual({
        version: 1,
        topics: ['topic-test'],
        userData: Buffer.alloc(0),
      })
    })
  })

  describe('MemberAssignment', () => {
    test('encode', () => {
      const buffer = MemberAssignment.encode({
        version: 1,
        assignment: { 'topic-test': [2, 5, 4, 1, 3, 0] },
      })

      expect(buffer).toEqual(FIXTURE_ROUND_ROBIN_ASSIGNER)
    })

    test('decode', () => {
      expect(MemberAssignment.decode(FIXTURE_ROUND_ROBIN_ASSIGNER)).toEqual({
        version: 1,
        assignment: { 'topic-test': [2, 5, 4, 1, 3, 0] },
        userData: Buffer.alloc(0),
      })
    })

    test('decode empty assignment', () => {
      expect(MemberAssignment.decode(Buffer.from([]))).toBe(null)
    })
  })
})
