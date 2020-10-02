const apiKeys = require('../../apiKeys')
const RequestV1Protocol = require('./request')

describe('Protocol > Requests > CreateAcls > v0', () => {
  let args

  beforeEach(() => {
    args = {
      creations: [
        {
          resourceType: 2,
          resourceName:
            'test-topic-119fe09ddb8092d6113d-15436-9fdcf583-7b77-4489-ac86-8af4a76ef420',
          principal: 'User:bob-575703bfac1e8c129332-15436-137b3edd-b741-4bb6-a266-318ac292beb8',
          host: '*',
          operation: 2,
          permissionType: 3,
        },
      ],
    }
  })

  test('metadata about the API', () => {
    const request = RequestV1Protocol(args)
    expect(request.apiKey).toEqual(apiKeys.CreateAcls)
    expect(request.apiVersion).toEqual(0)
    expect(request.apiName).toEqual('CreateAcls')
  })

  test('request', async () => {
    const { buffer } = await RequestV1Protocol(args).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
