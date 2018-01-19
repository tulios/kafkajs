const { decode, parse } = require('./response')

describe('Protocol > Requests > DescribeGroups > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      groups: [
        {
          errorCode: 0,
          groupId: 'consumer-group-id-608e7e42043d917ecb44',
          state: 'Stable',
          protocolType: 'consumer',
          protocol: 'default',
          members: [
            {
              memberId: 'test-5cc3bc27ca2660144976-fec6ade1-82ef-461e-81fe-c30e5908e2d2',
              clientId: 'test-5cc3bc27ca2660144976',
              clientHost: '/172.18.0.1',
              memberMetadata: Buffer.from([0, 0]),
              memberAssignment: Buffer.from('{}'),
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
