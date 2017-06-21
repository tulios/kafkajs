const Connection = require('../connection')
const loadAPI = require('./index')
const { secureRandom } = require('testHelpers')

describe('API > Produce', () => {
  let connection, api, topicName

  const createTopicData = () => [
    {
      topic: topicName,
      partitions: [
        {
          partition: 0,
          messages: [
            { key: `key-${secureRandom()}`, value: `some-value-${secureRandom()}` },
            { key: `key-${secureRandom()}`, value: `some-value-${secureRandom()}` },
            { key: `key-${secureRandom()}`, value: `some-value-${secureRandom()}` },
          ],
        },
      ],
    },
  ]

  beforeAll(async () => {
    topicName = `test-topic-${secureRandom()}`
    connection = new Connection({ host: 'localhost', port: 9092 })
    await connection.connect()
    api = await loadAPI(connection)
  })

  afterAll(() => {
    connection.disconnect()
  })

  test('request', async () => {
    await api.metadata([topicName])
    const response1 = await api.produce({ topicData: createTopicData() })
    expect(response1).toEqual({
      topics: [{ topicName, partitions: [{ errorCode: 0, offset: '0', partition: 0 }] }],
    })

    const response2 = await api.produce({ topicData: createTopicData() })
    expect(response2).toEqual({
      topics: [{ topicName, partitions: [{ errorCode: 0, offset: '3', partition: 0 }] }],
    })
  })
})
