const RequestV0Protocol = require('./request')
const CONFIG_RESOURCE_TYPES = require('../../../configResourceTypes')

describe('Protocol > Requests > DescribeConfigs > v0', () => {
  test('request', async () => {
    const { buffer } = await RequestV0Protocol({
      resources: [
        {
          type: CONFIG_RESOURCE_TYPES.TOPIC,
          name: 'test-topic-332d38bc4eee2ff29df6',
          configNames: ['compression.type', 'retention.ms'],
        },
      ],
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
