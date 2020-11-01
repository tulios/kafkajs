const { decode, parse } = require('./response')

describe('Protocol > Requests > Metadata > v6', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v5_response.json')))
    expect(data).toEqual({
      clientSideThrottleTime: 0,
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

  test('response with offline replicas', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v5_offline_replicas_response.json')))

    expect(data).toEqual({
      clientSideThrottleTime: 0,
      throttleTime: 0,
      brokers: [
        {
          nodeId: 1,
          host: '10.3.223.121',
          port: 9095,
          rack: null,
        },
        {
          nodeId: 0,
          host: '10.3.223.121',
          port: 9092,
          rack: null,
        },
      ],
      clusterId: 'tDr4DgFsS96sOEZu6e-N-Q',
      controllerId: 1,
      topicMetadata: [
        {
          topicErrorCode: 0,
          topic: 'topic-test',
          isInternal: false,
          partitionMetadata: [
            {
              isr: [],
              leader: -1,
              offlineReplicas: [2],
              partitionErrorCode: 5,
              partitionId: 2,
              replicas: [2],
            },
            {
              isr: [],
              leader: -1,
              offlineReplicas: [2],
              partitionErrorCode: 5,
              partitionId: 5,
              replicas: [2],
            },
            {
              isr: [1],
              leader: 1,
              offlineReplicas: [],
              partitionErrorCode: 0,
              partitionId: 4,
              replicas: [1],
            },
            {
              isr: [1],
              leader: 1,
              offlineReplicas: [],
              partitionErrorCode: 0,
              partitionId: 1,
              replicas: [1],
            },
            {
              isr: [0],
              leader: 0,
              offlineReplicas: [],
              partitionErrorCode: 0,
              partitionId: 3,
              replicas: [0],
            },
            {
              isr: [0],
              leader: 0,
              offlineReplicas: [],
              partitionErrorCode: 0,
              partitionId: 0,
              replicas: [0],
            },
          ],
        },
        {
          isInternal: false,
          partitionMetadata: [
            {
              isr: [],
              leader: -1,
              offlineReplicas: [2],
              partitionErrorCode: 5,
              partitionId: 0,
              replicas: [2],
            },
          ],
          topic: 'test-topic-tommy',
          topicErrorCode: 0,
        },
      ],
    })

    await expect(parse(data)).rejects.toThrow(
      'There is no leader for this topic-partition as we are in the middle of a leadership election'
    )
  })
})
