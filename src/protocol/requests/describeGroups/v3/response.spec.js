const { decode, parse } = require('./response')

describe('Protocol > Requests > DescribeGroups > v3', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v3_response.json')))
    expect(data).toEqual({
      throttleTime: 12,
      groups: [
        {
          errorCode: 0,
          groupId:
            'consumer-group-id-4de0aa10ef94403a397d-53384-d2fee969-1446-4166-bc8e-c88e8daffdfe',
          state: 'Stable',
          protocolType: 'protocol type',
          protocol: 'RoundRobinAssigner',
          members: [
            {
              memberId: 'member-1',
              clientId: 'client-1',
              clientHost: 'client-host-1',
              memberMetadata: null,
              memberAssignment: null,
            },
          ],
          authorizedOperations: 92,
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
