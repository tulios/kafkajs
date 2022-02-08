const RequestV1Protocol = require('./request')

describe('Protocol > Requests > CreateAcls > v1', () => {
  let args

  beforeEach(() => {
    args = {
      creations: [
        {
          resourceType: 2,
          resourceName:
            'test-topic-392850dd6d7a5d5b19ce-14472-4a2169c1-4784-4717-b2d1-9189bdfb8322',
          resourcePatternType: 3,
          principal: 'User:bob-4330407946585067d2b2-14472-2904446a-488c-4e40-8d24-cd7f758de713',
          host: '*',
          operation: 2,
          permissionType: 3,
        },
      ],
    }
  })

  test('request', async () => {
    const { buffer } = await RequestV1Protocol(args).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
