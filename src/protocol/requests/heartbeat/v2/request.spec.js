const RequestV0Protocol = require('./request')

describe('Protocol > Requests > Heartbeat > v1', () => {
  test('request', async () => {
    const { buffer } = await RequestV0Protocol({
      groupId: 'consumer-group-id-4c456000151f094b600d-26762-fd6a6ae7-3f66-408e-802e-d261d6983d0d',
      groupGenerationId: 1,
      memberId:
        'test-14da1b41ac688a6dcb78-26762-4dac8e12-dc28-4db2-8456-95bc6c1589bb-7bad1e84-c2de-4cc6-8071-badb27c86166',
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
