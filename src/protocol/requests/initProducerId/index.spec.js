const initProducerId = require('./index')
const requestV0 = require('./v0/request')

jest.mock('./v0/request')

describe('Protocol > Requests > InitProducerId', () => {
  describe('version v0', () => {
    test('provides a default timeout of 5000', async () => {
      initProducerId.protocol({ version: 0 })({ transactionalId: 'foo' })
      expect(requestV0).toHaveBeenCalledWith({ transactionalId: 'foo', transactionTimeout: 5000 })
    })
  })
})
