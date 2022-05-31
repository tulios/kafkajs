const RequestV2Protocol = require('./request')

describe('Protocol > Requests > FindCoordinator > v2', () => {
  test('request', async () => {
    const coordinatorKey = 'group-id'
    const coordinatorType = 0

    const { buffer } = await RequestV2Protocol({ coordinatorKey, coordinatorType }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
