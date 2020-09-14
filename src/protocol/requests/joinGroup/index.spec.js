const JoinGroupVersions = require('./index')
const JoinGroupV0 = JoinGroupVersions.protocol({ version: 0 })
const JoinGroupV1 = JoinGroupVersions.protocol({ version: 1 })
const JoinGroupV2 = JoinGroupVersions.protocol({ version: 2 })
const JoinGroupV3 = JoinGroupVersions.protocol({ version: 3 })
const JoinGroupV4 = JoinGroupVersions.protocol({ version: 4 })

describe('Protocol > Requests > JoinGroup', () => {
  describe('v0', () => {
    it('returns the requestTimeout', () => {
      const sessionTimeout = 30000
      const protocol = JoinGroupV0({
        groupId: 'test-group',
        sessionTimeout,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'default' }],
      })

      expect(protocol.requestTimeout).toBeGreaterThan(sessionTimeout)
    })

    it('does not use numbers large than MAX_SAFE_INTEGER', () => {
      const protocol = JoinGroupV0({
        groupId: 'test-group',
        sessionTimeout: Number.MAX_SAFE_INTEGER,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'default' }],
      })

      expect(protocol.requestTimeout).toEqual(Number.MAX_SAFE_INTEGER)
    })
  })

  describe('v1+', () => {
    it('uses the rebalanceTimeout for the requestTimeout when set', () => {
      const sessionTimeout = 1
      const rebalanceTimeout = 30000
      const parameters = {
        groupId: 'test-group',
        sessionTimeout,
        rebalanceTimeout,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'default' }],
      }
      const protocolV1 = JoinGroupV1(parameters)
      const protocolV2 = JoinGroupV2(parameters)
      const protocolV3 = JoinGroupV3(parameters)
      const protocolV4 = JoinGroupV4(parameters)

      expect(protocolV1.requestTimeout).toBeGreaterThan(rebalanceTimeout)
      expect(protocolV2.requestTimeout).toBeGreaterThan(rebalanceTimeout)
      expect(protocolV3.requestTimeout).toBeGreaterThan(rebalanceTimeout)
      expect(protocolV4.requestTimeout).toBeGreaterThan(rebalanceTimeout)
    })
  })

  describe('v4+', () => {
    it('does not log error responses when memberId is empty', () => {
      const protocol = JoinGroupV4({
        groupId: 'test-group',
        sessionTimeout: 1,
        rebalanceTimeout: 30000,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'default' }],
      })

      expect(protocol.logResponseError).toEqual(false)
    })

    it('logs error responses when memberId is not empty', () => {
      const protocol = JoinGroupV4({
        groupId: 'test-group',
        sessionTimeout: 1,
        rebalanceTimeout: 30000,
        memberId: 'member-id',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'default' }],
      })

      expect(protocol.logResponseError).toEqual(true)
    })
  })
})
