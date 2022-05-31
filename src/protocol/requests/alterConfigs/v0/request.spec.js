const RequestV0Protocol = require('./request')
const CONFIG_RESOURCE_TYPES = require('../../../configResourceTypes')

describe('Protocol > Requests > AlterConfigs > v0', () => {
  test('request', async () => {
    const { buffer } = await RequestV0Protocol({
      resources: [
        {
          type: CONFIG_RESOURCE_TYPES.TOPIC,
          name: 'test-topic-d7fa92c03177d87573b1-38076-21364f66-8613-47e0-b273-bc9de397515e',
          configEntries: [{ name: 'cleanup.policy', value: 'compact' }],
        },
      ],
      validateOnly: false,
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v0_request.json')))
  })
})
