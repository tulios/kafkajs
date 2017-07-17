const createProducer = require('./index')
const { secureRandom, connectionOpts, createModPartitioner } = require('testHelpers')

describe('Producer', () => {
  let topicName, producer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    producer = createProducer(
      Object.assign(connectionOpts(), {
        createPartitioner: createModPartitioner,
      })
    )

    await producer.connect()
  })

  afterEach(async () => {
    await producer.disconnect()
  })

  test('produce messages', async () => {
    const sendMessages = async () =>
      await producer.send({
        topic: topicName,
        messages: new Array(10).fill().map((_, i) => ({
          key: `key-${i}`,
          value: `value-${i}`,
        })),
      })

    expect(await sendMessages()).toEqual([
      {
        topicName,
        errorCode: 0,
        offset: '0',
        partition: 0,
        timestamp: '-1',
      },
    ])

    expect(await sendMessages()).toEqual([
      {
        topicName,
        errorCode: 0,
        offset: '10',
        partition: 0,
        timestamp: '-1',
      },
    ])
  })
})
