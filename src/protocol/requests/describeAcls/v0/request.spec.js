const RequestV0Protocol = require('./request')

describe('Protocol > Requests > DescribeAcls > v0', () => {
  let args

  beforeEach(() => {
    args = {
      resourceType: 2,
      resourceName: 'test-topic-064a1bcf62c877843e3c-18742-da400056-f741-4b3e-a725-b758d8104afa',
      host: '*',
      operation: 2,
      permissionType: 3,
    }
  })

  test('request', async () => {
    const { buffer } = await RequestV0Protocol(args).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
