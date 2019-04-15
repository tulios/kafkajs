const RequestV1Protocol = require('./request')

describe('Protocol > Requests > JoinGroup > v1', () => {
  test('request', async () => {
    const { buffer } = await RequestV1Protocol({
      groupId: 'consumer-group-id-5d520373e1cf4d03ca77-21486-90948f57-528c-4c3b-ba72-bf1e0d9bbc56',
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      memberId: '',
      protocolType: 'consumer',
      groupProtocols: [
        {
          name: 'AssignerName',
          metadata: Buffer.from(require('../fixtures/v1_assignerMetadata.json')),
        },
      ],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
