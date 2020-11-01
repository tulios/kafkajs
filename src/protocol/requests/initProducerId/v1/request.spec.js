const RequestV1Protocol = require('./request')

describe('Protocol > Requests > InitProducerId > v1', () => {
  test('request', async () => {
    const transactionalId = 'initproduceridtransaction'
    const transactionTimeout = 30000

    const { buffer } = await RequestV1Protocol({ transactionalId, transactionTimeout }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
