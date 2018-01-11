const RequestV0Protocol = require('../v0/request')

describe('Protocol > Requests > DescribeGroups > v0', () => {
  test('request', async () => {
    const groupIds = ['consumer-group-id-608e7e42043d917ecb44']
    const { buffer } = await RequestV0Protocol({ groupIds }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
