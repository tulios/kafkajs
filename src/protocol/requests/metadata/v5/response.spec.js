const { decode, parse } = require('./response')

describe('Protocol > Requests > Metadata > v5', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v5_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      brokers: [
        {
          nodeId: 2,
          host: '10.3.220.89',
          port: 9098,
          rack: null,
        },
        {
          nodeId: 1,
          host: '10.3.220.89',
          port: 9095,
          rack: null,
        },
        {
          nodeId: 0,
          host: '10.3.220.89',
          port: 9092,
          rack: null,
        },
      ],
      clusterId: 'wyOEk0m7Tn-08oGZjtVgEg',
      controllerId: 2,
      topicMetadata: [
        {
          topicErrorCode: 0,
          topic: 'test-topic-f5e17a86896ebfdeb429-80829-a37b6dde-1adc-4687-813d-52d75a0a0f78',
          isInternal: false,
          partitionMetadata: [
            {
              partitionErrorCode: 0,
              partitionId: 0,
              leader: 0,
              replicas: [0],
              isr: [0],
              offlineReplicas: [],
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
