const RequestV0Protocol = require('./request')

describe('Protocol > Requests > LeaveGroup > v0', () => {
  test('request', async () => {
    const groupId = 'consumer-group-id-64fbf5dce5065868aa8f'
    const memberId = 'test-45eb7a4239f548578e8b-b2b08fa3-b887-4719-b9e1-391ec944b53f'

    const { buffer } = await RequestV0Protocol({ groupId, memberId }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
