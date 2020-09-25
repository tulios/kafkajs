const RequestV3Protocol = require('./request')

describe('Protocol > Requests > LeaveGroup > v3', () => {
  test('request', async () => {
    const { buffer } = await RequestV3Protocol({
      groupId: 'consumer-group-id-82d77df5d0974e21502d-30919-0ec5e55e-e3e1-433a-bbed-96fe228408b4',
      members: [
        {
          memberId:
            'test-c598169a5d8dbedcb806-30919-ff1f3c53-1855-4c04-aadf-12d298160f5c-b41b37f8-6482-47c5-811e-e658ab656a75',
          groupInstanceId: null,
        },
      ],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v3_request.json')))
  })
})
