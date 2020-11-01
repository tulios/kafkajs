const RequestV2Protocol = require('./request')

describe('Protocol > Requests > DescribeConfigs > v2', () => {
  test('request', async () => {
    const { buffer } = await RequestV2Protocol({
      includeSynonyms: true,
      resources: [
        {
          type: 2,
          name: 'test-topic-e0cadb9e9f1a6396c116-54438-43bb8b69-32cf-4909-af02-cbe20c2d9e3d',
          configNames: ['compression.type', 'retention.ms'],
        },
      ],
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v1_request.json')))
  })
})
