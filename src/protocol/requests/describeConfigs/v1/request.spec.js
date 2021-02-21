const RequestV1Protocol = require('./request')
const ConfigResourceTypes = require('../../../configResourceTypes')

describe('Protocol > Requests > DescribeConfigs > v1', () => {
  test('request', async () => {
    const { buffer } = await RequestV1Protocol({
      includeSynonyms: true,
      resources: [
        {
          type: ConfigResourceTypes.TOPIC,
          name: 'topic-test1',
          configNames: ['compression.type', 'retention.ms'],
        },
      ],
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
