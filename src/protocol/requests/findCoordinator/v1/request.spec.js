const RequestV0Protocol = require('./request')

describe('Protocol > Requests > FindCoordinator > v1', () => {
  test('request', async () => {
    const coordinatorKey = 'group-id'
    const coordinatorType = 0

    const { buffer } = await RequestV0Protocol({ coordinatorKey, coordinatorType }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
