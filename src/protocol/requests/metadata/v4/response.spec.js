const { decode, parse } = require('./response')

describe('Protocol > Requests > Metadata > v4', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response.json')))
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
          topic: 'test-topic-66d1df55ec27e033d726',
          isInternal: false,
          partitionMetadata: [
            {
              partitionErrorCode: 0,
              partitionId: 0,
              leader: 1,
              replicas: [1],
              isr: [1],
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
