const loadAPI = require('./index')
const { secureRandom, createConnection } = require('testHelpers')

describe('API > Metadata', () => {
  let connection, topicName, api

  beforeAll(async () => {
    topicName = `test-topic-${secureRandom()}`
    connection = createConnection()
    await connection.connect()
    api = await loadAPI(connection)
  })

  afterAll(() => {
    connection.disconnect()
  })

  test('request', async () => {
    const response = await api.metadata([topicName])
    expect(response).toEqual({
      brokers: [
        {
          host: expect.stringMatching(/\d+\.\d+\.\d+\.\d+/),
          nodeId: expect.any(Number),
          port: expect.any(Number),
          rack: null,
        },
        {
          host: expect.stringMatching(/\d+\.\d+\.\d+\.\d+/),
          nodeId: expect.any(Number),
          port: expect.any(Number),
          rack: null,
        },
        {
          host: expect.stringMatching(/\d+\.\d+\.\d+\.\d+/),
          nodeId: expect.any(Number),
          port: expect.any(Number),
          rack: null,
        },
      ],
      clusterId: expect.stringMatching(/[a-zA-Z0-9\-]/),
      controllerId: expect.any(Number),
      topicMetadata: [
        {
          isInternal: false,
          partitionMetadata: [
            {
              isr: expect.any(Number),
              leader: expect.any(Number),
              partitionErrorCode: 0,
              partitionId: 0,
              replicas: 1,
            },
          ],
          topic: topicName,
          topicErrorCode: 0,
        },
      ],
    })
  })
})
