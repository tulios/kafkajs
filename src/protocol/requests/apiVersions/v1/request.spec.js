const RequestV0Protocol = require('./request')

describe('Protocol > Requests > ApiVersions > v1', () => {
  test('request', async () => {
    const { buffer } = await RequestV0Protocol().encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
