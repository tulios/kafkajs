const { decode, parse } = require('./response')

describe('Protocol > Requests > Metadata > v3', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v3_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      brokers: [
        {
          nodeId: 2,
          host: '192.168.1.173',
          port: 9098,
          rack: null,
        },
        {
          nodeId: 1,
          host: '192.168.1.173',
          port: 9095,
          rack: null,
        },
        {
          nodeId: 0,
          host: '192.168.1.173',
          port: 9092,
          rack: null,
        },
      ],
      clusterId: 'Q0WO3u_TTAeslFDJWiiGvA',
      controllerId: 1,
      topicMetadata: [
        {
          topicErrorCode: 0,
          topic: 'test-topic-0f67c79007c9157fc83d',
          isInternal: false,
          partitionMetadata: [
            {
              partitionErrorCode: 0,
              partitionId: 0,
              leader: 2,
              replicas: [2],
              isr: [2],
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
