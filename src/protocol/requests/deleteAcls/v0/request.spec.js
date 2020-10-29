const RequestV0Protocol = require('./request')

describe('Protocol > Requests > DeleteAcls > v0', () => {
  let args

  beforeEach(() => {
    args = {
      filters: [
        {
          resourceName:
            'test-topic-78d599e9d78a4da685ae-21381-e8f39f07-7d19-4677-aecb-bd0f731f1e28',
          resourceType: 2,
          resourcePatternType: 3,
          principal: 'User:bob-cd8856cf4f23fe19899c-21381-c20b6340-b95c-431d-9237-2f15e310fba7',
          host: '*',
          permissionType: 3,
          operation: 1,
        },
      ],
    }
  })

  test('request', async () => {
    const { buffer } = await RequestV0Protocol(args).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
