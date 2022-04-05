const RequestV5Protocol = require('./request')

describe('Protocol > Requests > JoinGroup > v5', () => {
  test('request', async () => {
    const { buffer } = await RequestV5Protocol({
      groupId: 'consumer-group-id-b522188a3a12a1f04cfb-23702-e1ff35c7-fde9-4d58-960a-2cef8af77eef',
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      memberId: '',
      groupInstanceId: 'group-instance-id',
      protocolType: 'consumer',
      groupProtocols: [
        {
          name: 'AssignerName',
          metadata: Buffer.from(require('../fixtures/v2_assignerMetadata.json')),
        },
      ],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v5_request.json')))
  })
})
