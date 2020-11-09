const { decode, parse } = require('./response')

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
          resourceType: 2,
          resourceName:
            'test-topic-e0cadb9e9f1a6396c116-54438-43bb8b69-32cf-4909-af02-cbe20c2d9e3d',
          configEntries: [
            {
              configName: 'compression.type',
              configValue: 'producer',
              readOnly: false,
              isDefault: false,
              isSensitive: false,
              configSynonyms: [
                {
                  configName: 'compression.type',
                  configSource: 5,
                  configValue: 'producer',
                },
              ],
            },
            {
              configName: 'retention.ms',
              configValue: '604800000',
              readOnly: false,
              isDefault: false,
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
