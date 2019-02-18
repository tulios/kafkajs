const JoinGroupVersions = require('./index')
const JoinGroup = JoinGroupVersions.protocol({ version: 0 })

describe('Protocol > Requests > JoinGroup', () => {
  describe('v0', () => {
    it('returns the requestTimeout', () => {
      const sessionTimeout = 30000
      const protocol = JoinGroup({
        groupId: 'test-group',
        sessionTimeout,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'default' }],
      })

      expect(protocol.requestTimeout).toBeGreaterThan(sessionTimeout)
    })

    it('does not use numbers large than MAX_SAFE_INTEGER', () => {
      const protocol = JoinGroup({
        groupId: 'test-group',
        sessionTimeout: Number.MAX_SAFE_INTEGER,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'default' }],
      })

      expect(protocol.requestTimeout).toEqual(Number.MAX_SAFE_INTEGER)
    })
  })
})
