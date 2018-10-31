const RequestV0Protocol = require('./request')

describe('Protocol > Requests > InitProducerId > v0', () => {
  test('request', async () => {
    const transactionalId = 'initproduceridtransaction'
    const transactionTimeout = 30000

    const { buffer } = await RequestV0Protocol({ transactionalId, transactionTimeout }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
