const apiKeys = require('../../apiKeys')
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

  test('metadata about the API', () => {
    const request = RequestV1Protocol(args)
    expect(request.apiKey).toEqual(apiKeys.DeleteAcls)
    expect(request.apiVersion).toEqual(1)
    expect(request.apiName).toEqual('DeleteAcls')
  })

  test('request', async () => {
    const { buffer } = await RequestV1Protocol(args).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
