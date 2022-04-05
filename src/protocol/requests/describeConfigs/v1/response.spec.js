const { decode, parse } = require('./response')
const { DEFAULT_CONFIG } = require('../../../configSource')

describe('Protocol > Requests > DescribeConfigs > v1', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2,
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
