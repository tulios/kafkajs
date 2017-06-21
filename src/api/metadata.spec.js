const loadAPI = require('./index')
const { createConnection } = require('testHelpers')

describe('API > Metadata', () => {
  let connection, api

  beforeAll(async () => {
    connection = createConnection()
    await connection.connect()
    api = await loadAPI(connection)
  })

  afterAll(() => {
    connection.disconnect()
  })

  test('request', async () => {
    const response = await api.metadata(['test-topic-1'])
    expect(response).toEqual({
      brokers: [{ host: 'localhost', nodeId: 0, port: 9092, rack: null }],
      clusterId: expect.stringMatching(/[a-zA-Z0-9]-[a-zA-Z0-9]/),
      controllerId: 0,
      topicMetadata: [
        {
          isInternal: false,
          partitionMetadata: [
            { isr: 0, leader: 0, partitionErrorCode: 0, partitionId: 0, replicas: 1 },
          ],
          topic: 'test-topic-1',
          topicErrorCode: 0,
        },
      ],
    })
  })
})
