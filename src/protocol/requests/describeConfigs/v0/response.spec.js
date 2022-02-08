const { decode, parse } = require('./response')
const ConfigSource = require('../../../configSource')

describe('Protocol > Requests > DescribeConfigs > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      resources: [
        {
          errorCode: 0,
          errorMessage: null,
          resourceType: 2,
          resourceName: 'test-topic-443a0ba6d66fd2161c73',
          configEntries: [
            {
              configName: 'compression.type',
              configValue: 'producer',
              readOnly: false,
              isDefault: true,
              configSource: ConfigSource.DEFAULT_CONFIG,
              isSensitive: false,
            },
            {
              configName: 'retention.ms',
              configValue: '604800000',
              readOnly: false,
              isDefault: true,
              configSource: ConfigSource.DEFAULT_CONFIG,
              isSensitive: false,
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  describe('ConfigSource backporting', () => {
    it('should be set to TOPIC_CONFIG when the altered config belongs to a topic resource', async () => {
      const data = await decode(Buffer.from(require('../fixtures/v0_response_topic_config.json')))
      const { configEntries } = data.resources[0]

      expect(configEntries).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            configName: 'retention.ms',
            isDefault: false,
            configSource: ConfigSource.TOPIC_CONFIG,
          }),
        ])
      )
    })

    it('should be set to STATIC_BROKER_CONFIG when the altered config belongs to a broker resource', async () => {
      const data = await decode(Buffer.from(require('../fixtures/v0_response_broker_config.json')))
      const { configEntries } = data.resources[0]

      expect(configEntries).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            configName: 'sasl.kerberos.service.name',
            isDefault: false,
            configSource: ConfigSource.STATIC_BROKER_CONFIG,
          }),
        ])
      )
    })
  })
})
