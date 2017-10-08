const createProducer = require('./index')
const {
  secureRandom,
  connectionOpts,
  sslConnectionOpts,
  createModPartitioner,
} = require('testHelpers')

describe('Producer', () => {
  let topicName, producer

  beforeEach(() => {
    topicName = `test-topic-${secureRandom()}`
  })

  afterEach(async () => {
    await producer.disconnect()
  })

  test('support SSL connections', async () => {
    producer = createProducer(sslConnectionOpts())
    await producer.connect()
  })

  test('produce messages', async () => {
    producer = createProducer(
      Object.assign(connectionOpts(), {
        createPartitioner: createModPartitioner,
      })
    )

    await producer.connect()

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
