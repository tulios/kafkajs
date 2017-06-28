const Broker = require('./index')
const loadApiVersions = require('./apiVersions')
const { secureRandom, createConnection } = require('testHelpers')

describe('Broker > Metadata', () => {
  let connection, topicName, versions, broker

  beforeAll(async () => {
    topicName = `test-topic-${secureRandom()}`
    connection = createConnection()
    await connection.connect()
    versions = await loadApiVersions(connection)
    broker = new Broker(connection, versions)
  })

  afterAll(() => {
    connection.disconnect()
  })

  test('request', async () => {
    const response = await broker.metadata([topicName])
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
              isr: expect.any(Array),
              leader: expect.any(Number),
              partitionErrorCode: 0,
              partitionId: 0,
              replicas: expect.any(Array),
            },
          ],
          topic: topicName,
          topicErrorCode: 0,
        },
      ],
    })
  })
})
