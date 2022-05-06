const { decode, parse } = require('./response')
const CONFIG_RESOURCE_TYPES = require('../../../configResourceTypes')

describe('Protocol > Requests > AlterConfigs > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceName:
            'test-topic-d7fa92c03177d87573b1-38076-21364f66-8613-47e0-b273-bc9de397515e',
          resourceType: CONFIG_RESOURCE_TYPES.TOPIC,
        },
      ],
      throttleTime: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
