const RequestV0Protocol = require('./request')

describe('Protocol > Requests > FindCoordinator > v0', () => {
  test('request', () => {
    const groupId = 'test-topic'

    const { buffer } = RequestV0Protocol({ groupId }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
