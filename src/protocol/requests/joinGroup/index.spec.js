const JoinGroupVersions = require('./index')
const JoinGroupV0 = JoinGroupVersions.protocol({ version: 0 })
const JoinGroupV1 = JoinGroupVersions.protocol({ version: 1 })
const JoinGroupV2 = JoinGroupVersions.protocol({ version: 2 })

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

      expect(protocolV1.requestTimeout).toBeGreaterThan(rebalanceTimeout)
      expect(protocolV2.requestTimeout).toBeGreaterThan(rebalanceTimeout)
    })
  })
})
