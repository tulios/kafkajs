const RequestV0Protocol = require('./request')

describe('Protocol > Requests > Heartbeat > v0', () => {
  test('request', async () => {
    const groupId = 'consumer-group-id-ba8da1f6117562ed5615'
    const memberId = 'test-169232b069c4a377bc4b-040f5f1a-a469-4062-9d36-bd803d8d6767'
    const groupGenerationId = 1

    const { buffer } = await RequestV0Protocol({ groupId, groupGenerationId, memberId }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
