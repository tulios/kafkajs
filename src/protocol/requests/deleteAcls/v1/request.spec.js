const RequestV1Protocol = require('./request')

describe('Protocol > Requests > DeleteAcls > v1', () => {
  let args

  beforeEach(() => {
    args = {
      filters: [
        {
          resourceName:
            'test-topic-000b0fa9008f920bc684-20826-6bcf579e-e882-47b8-9586-e778588f9e78',
          resourceType: 2,
          resourcePatternType: 3,
          principal: 'User:bob-51fe15d9fc1c5a3be5f2-20826-fcf12830-b5a1-477a-8ac9-866a4088273a',
          host: '*',
          permissionType: 3,
          operation: 1,
        },
      ],
    }
  })

  test('request', async () => {
    const { buffer } = await RequestV1Protocol(args).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
