const RequestV1Protocol = require('./request')

describe('Protocol > Requests > DescribeAcls > v1', () => {
  let args

  beforeEach(() => {
    args = {
      resourceType: 2,
      resourceName: 'test-topic-3091e37cb34e1e916cfa-18029-1b277b41-4f40-4740-9274-51f556f212c9',
      resourcePatternType: 3,
      host: '*',
      operation: 2,
      permissionType: 3,
    }
  })

  test('request', async () => {
    const { buffer } = await RequestV1Protocol(args).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
