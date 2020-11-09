const { decode, parse } = require('./response')

describe('Protocol > Requests > DescribeAcls > v1', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      errorCode: 0,
      errorMessage: null,
      resources: [
        {
          resourceType: 2,
          resourceName:
            'test-topic-3091e37cb34e1e916cfa-18029-1b277b41-4f40-4740-9274-51f556f212c9',
          resourcePatternType: 3,
          acls: [
            {
              principal: 'User:bob-bbc9e8f21ca0d1e60eba-18029-e0cee136-6f05-43fc-a235-26a779e72413',
              host: '*',
              operation: 2,
              permissionType: 3,
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
