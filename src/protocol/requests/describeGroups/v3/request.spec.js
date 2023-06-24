const RequestV2Protocol = require('./request')

describe('Protocol > Requests > DescribeGroups > v3', () => {
  test('request', async () => {
    const { buffer } = await RequestV2Protocol({
      groupIds: [
        'consumer-group-id-4de0aa10ef94403a397d-53384-d2fee969-1446-4166-bc8e-c88e8daffdfe',
      ],
      includeAuthorizedOperations: true,
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v3_request.json')))
  })
})
