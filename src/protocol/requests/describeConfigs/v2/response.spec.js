const { decode, parse } = require('./response')
const ConfigResourceTypes = require('../../../configResourceTypes')
const { DEFAULT_CONFIG } = require('../../../configSource')

describe('Protocol > Requests > DescribeConfigs > v2', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'topic-test1',
          configEntries: [
            {
              configName: 'compression.type',
              configValue: 'producer',
              readOnly: false,
              isDefault: true,
              configSource: DEFAULT_CONFIG,
              isSensitive: false,
              configSynonyms: [
                {
                  configName: 'compression.type',
                  configSource: DEFAULT_CONFIG,
                  configValue: 'producer',
                },
              ],
            },
            {
              configName: 'retention.ms',
              configValue: '604800000',
              readOnly: false,
              isDefault: true,
              configSource: DEFAULT_CONFIG,
              isSensitive: false,
              configSynonyms: [],
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
