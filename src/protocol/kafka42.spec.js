const { errorCodes, createErrorFromCode } = require('./error')
const apiKeys = require('./requests/apiKeys')
const { requests } = require('./requests')

describe('Protocol > Kafka 4.2 Support', () => {
  describe('API Keys', () => {
    it('registers Share Group API keys (KIP-932)', () => {
      expect(apiKeys.ShareGroupHeartbeat).toBe(76)
      expect(apiKeys.ShareGroupDescribe).toBe(77)
      expect(apiKeys.ShareFetch).toBe(78)
      expect(apiKeys.ShareAcknowledge).toBe(79)
    })

    it('registers KRaft API keys', () => {
      expect(apiKeys.DescribeQuorum).toBe(55)
      expect(apiKeys.Vote).toBe(52)
      expect(apiKeys.BeginQuorumEpoch).toBe(53)
      expect(apiKeys.EndQuorumEpoch).toBe(54)
      expect(apiKeys.FetchSnapshot).toBe(59)
      expect(apiKeys.BrokerRegistration).toBe(62)
      expect(apiKeys.BrokerHeartbeat).toBe(63)
      expect(apiKeys.ControllerRegistration).toBe(70)
    })

    it('registers Kafka 3.x API keys', () => {
      expect(apiKeys.OffsetDelete).toBe(47)
      expect(apiKeys.DescribeClientQuotas).toBe(48)
      expect(apiKeys.AlterClientQuotas).toBe(49)
      expect(apiKeys.DescribeUserScramCredentials).toBe(50)
      expect(apiKeys.AlterUserScramCredentials).toBe(51)
      expect(apiKeys.DescribeCluster).toBe(60)
      expect(apiKeys.DescribeProducers).toBe(61)
      expect(apiKeys.DescribeTransactions).toBe(65)
      expect(apiKeys.ListTransactions).toBe(66)
    })

    it('registers consumer group protocol API keys', () => {
      expect(apiKeys.ConsumerGroupHeartbeat).toBe(68)
      expect(apiKeys.ConsumerGroupDescribe).toBe(69)
    })

    it('registers telemetry API keys', () => {
      expect(apiKeys.GetTelemetrySubscriptions).toBe(71)
      expect(apiKeys.PushTelemetry).toBe(72)
    })

    it('registers Kafka 4.1-4.2 API keys', () => {
      expect(apiKeys.AddRaftVoter).toBe(80)
      expect(apiKeys.RemoveRaftVoter).toBe(81)
      expect(apiKeys.UpdateRaftVoter).toBe(82)
      expect(apiKeys.ReadShareGroupStateSummary).toBe(83)
    })

    it('registers DescribeTopicPartitions API key', () => {
      expect(apiKeys.DescribeTopicPartitions).toBe(75)
    })

    it('has request definitions for all registered request names', () => {
      const requestNames = Object.keys(requests)

      // Every request in the registry should have a corresponding entry
      for (const name of requestNames) {
        expect(requests[name]).toBeTruthy()
        expect(requests[name]).toHaveProperty('versions')
        expect(requests[name]).toHaveProperty('protocol')
      }
    })
  })

  describe('Error Codes', () => {
    it('includes Kafka 2.6+ error codes', () => {
      const throttlingError = errorCodes.find(e => e.type === 'THROTTLING_QUOTA_EXCEEDED')
      expect(throttlingError).toBeTruthy()
      expect(throttlingError.code).toBe(89)
      expect(throttlingError.retriable).toBe(true)

      const producerFenced = errorCodes.find(e => e.type === 'PRODUCER_FENCED')
      expect(producerFenced).toBeTruthy()
      expect(producerFenced.code).toBe(90)
      expect(producerFenced.retriable).toBe(false)
    })

    it('includes Kafka 3.0+ error codes', () => {
      const unknownTopicId = errorCodes.find(e => e.type === 'UNKNOWN_TOPIC_ID')
      expect(unknownTopicId).toBeTruthy()
      expect(unknownTopicId.code).toBe(100)
      expect(unknownTopicId.retriable).toBe(true)

      const inconsistentTopicId = errorCodes.find(e => e.type === 'INCONSISTENT_TOPIC_ID')
      expect(inconsistentTopicId).toBeTruthy()
      expect(inconsistentTopicId.code).toBe(103)

      const txnIdNotFound = errorCodes.find(e => e.type === 'TRANSACTIONAL_ID_NOT_FOUND')
      expect(txnIdNotFound).toBeTruthy()
      expect(txnIdNotFound.code).toBe(105)
    })

    it('includes Kafka 3.6+ consumer group protocol error codes', () => {
      const fencedMember = errorCodes.find(e => e.type === 'FENCED_MEMBER_EPOCH')
      expect(fencedMember).toBeTruthy()
      expect(fencedMember.code).toBe(110)

      const unreleasedInstance = errorCodes.find(e => e.type === 'UNRELEASED_INSTANCE_ID')
      expect(unreleasedInstance).toBeTruthy()
      expect(unreleasedInstance.code).toBe(111)

      const unsupportedAssignor = errorCodes.find(e => e.type === 'UNSUPPORTED_ASSIGNOR')
      expect(unsupportedAssignor).toBeTruthy()
      expect(unsupportedAssignor.code).toBe(112)
    })

    it('includes Kafka 4.0+ Share Group error codes (KIP-932)', () => {
      const shareSessionNotFound = errorCodes.find(e => e.type === 'SHARE_SESSION_NOT_FOUND')
      expect(shareSessionNotFound).toBeTruthy()
      expect(shareSessionNotFound.code).toBe(120)
      expect(shareSessionNotFound.retriable).toBe(true)

      const invalidShareSession = errorCodes.find(e => e.type === 'INVALID_SHARE_SESSION_EPOCH')
      expect(invalidShareSession).toBeTruthy()
      expect(invalidShareSession.code).toBe(121)
      expect(invalidShareSession.retriable).toBe(true)

      const fencedState = errorCodes.find(e => e.type === 'FENCED_STATE_EPOCH')
      expect(fencedState).toBeTruthy()
      expect(fencedState.code).toBe(122)
      expect(fencedState.retriable).toBe(true)
    })

    it('includes Kafka 4.1+ error codes', () => {
      const duplicateVoter = errorCodes.find(e => e.type === 'DUPLICATE_VOTER')
      expect(duplicateVoter).toBeTruthy()
      expect(duplicateVoter.code).toBe(124)

      const voterNotFound = errorCodes.find(e => e.type === 'VOTER_NOT_FOUND')
      expect(voterNotFound).toBeTruthy()
      expect(voterNotFound.code).toBe(125)
    })

    it('includes Kafka 4.2+ error codes', () => {
      const invalidRegex = errorCodes.find(e => e.type === 'INVALID_REGULAR_EXPRESSION')
      expect(invalidRegex).toBeTruthy()
      expect(invalidRegex.code).toBe(126)
      expect(invalidRegex.retriable).toBe(false)

      const unknownShareState = errorCodes.find(e => e.type === 'UNKNOWN_SHARE_GROUP_STATE')
      expect(unknownShareState).toBeTruthy()
      expect(unknownShareState.code).toBe(127)
      expect(unknownShareState.retriable).toBe(false)
    })

    it('creates proper error objects for all new error codes', () => {
      const newCodes = [89, 90, 91, 92, 100, 103, 105, 110, 111, 112, 120, 121, 122, 126, 127]
      for (const code of newCodes) {
        const error = createErrorFromCode(code)
        expect(error).toBeTruthy()
        expect(error.type).not.toBe('KAFKAJS_UNKNOWN_ERROR_CODE')
        expect(error.code).toBe(code)
      }
    })

    it('has no gaps in error code coverage from 89 to 127', () => {
      for (let code = 89; code <= 127; code++) {
        const error = createErrorFromCode(code)
        expect(error).toBeTruthy()
        expect(error.type).not.toBe('KAFKAJS_UNKNOWN_ERROR_CODE')
      }
    })

    it('has unique error codes', () => {
      const codes = errorCodes.map(e => e.code)
      const uniqueCodes = new Set(codes)
      expect(codes.length).toBe(uniqueCodes.size)
    })

    it('has unique error types', () => {
      const types = errorCodes.map(e => e.type)
      const uniqueTypes = new Set(types)
      expect(types.length).toBe(uniqueTypes.size)
    })
  })

  describe('staleMetadata', () => {
    const { staleMetadata } = require('./error')

    it('recognizes UNKNOWN_TOPIC_ID as stale metadata', () => {
      expect(staleMetadata({ type: 'UNKNOWN_TOPIC_ID' })).toBe(true)
    })

    it('still recognizes classic stale metadata errors', () => {
      expect(staleMetadata({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })).toBe(true)
      expect(staleMetadata({ type: 'LEADER_NOT_AVAILABLE' })).toBe(true)
      expect(staleMetadata({ type: 'NOT_LEADER_FOR_PARTITION' })).toBe(true)
    })
  })
})
