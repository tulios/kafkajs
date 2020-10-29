const initProducerId = require('./index')
const requestV0 = require('./v0/request')
const requestV1 = require('./v1/request')

jest.mock('./v0/request')
jest.mock('./v1/request')

describe('Protocol > Requests > InitProducerId', () => {
  ;[requestV0, requestV1].forEach((request, version) => {
    describe(`version v${version}`, () => {
      test('provides a default timeout of 5000', async () => {
        initProducerId.protocol({ version })({ transactionalId: 'foo' })
        expect(request).toHaveBeenCalledWith({ transactionalId: 'foo', transactionTimeout: 5000 })
      })
    })
  })
})
