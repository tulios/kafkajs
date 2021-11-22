const {
  createCluster,
  secureRandom,
  createTopic,
  createModPartitioner,
  newLogger,
} = require('testHelpers')

const createProducer = require('../../producer')

describe('Cluster > fetchTopicsOffset', () => {
  let cluster, topic, producer

  const sendSampleMessages = async () => {
    await producer.send({
      topic,
      acks: 1,
      messages: [
        { key: 'k1', value: 'v1' },
        { key: 'k2', value: 'v2' },
        { key: 'k3', value: 'v3' },
        { key: 'k4', value: 'v4' },
      ],
    })
  }

  beforeEach(async () => {
    topic = `test-topic-${secureRandom()}`
    cluster = createCluster()

    await createTopic({ topic, partitions: 3 })
    await cluster.connect()
    await cluster.addTargetTopic(topic)

    producer = createProducer({
      cluster,
      logger: newLogger(),
      createPartitioner: createModPartitioner,
    })

    await producer.connect()
    await sendSampleMessages()
  })

  afterEach(async () => {
    producer && (await producer.disconnect())
    cluster && (await cluster.disconnect())
  })

  test('returns latest offsets by default', async () => {
    const result = await cluster.fetchTopicsOffset([
      {
        topic,
        partitions: [{ partition: 0 }, { partition: 1 }, { partition: 2 }],
      },
    ])

    expect(result).toEqual([
      {
        topic,
        partitions: expect.arrayContaining([
          { partition: 0, offset: '1' },
          { partition: 1, offset: '2' },
          { partition: 2, offset: '1' },
        ]),
      },
    ])
  })

  test('returns earliest if fromBeginning=true', async () => {
    const result = await cluster.fetchTopicsOffset([
      { topic, partitions: [{ partition: 0 }], fromBeginning: true },
    ])

    expect(result).toEqual([{ topic, partitions: [{ partition: 0, offset: '0' }] }])
  })

  test('returns correct offsets if fromTimestamp', async () => {
    const fromTimestamp = Date.now()
    await sendSampleMessages()
    const resultTimestamp = await cluster.fetchTopicsOffset([
      {
        topic,
        fromTimestamp,
        partitions: [{ partition: 0 }, { partition: 1 }, { partition: 2 }],
      },
    ])

    expect(resultTimestamp).toEqual([
      {
        topic,
        partitions: expect.arrayContaining([
          { partition: 0, offset: '1' },
          { partition: 1, offset: '2' },
          { partition: 2, offset: '1' },
        ]),
      },
    ])

    const result = await cluster.fetchTopicsOffset([
      {
        topic,
        partitions: [{ partition: 0 }, { partition: 1 }, { partition: 2 }],
      },
    ])

    expect(result).toEqual([
      {
        topic,
        partitions: expect.arrayContaining([
          { partition: 0, offset: '2' },
          { partition: 1, offset: '4' },
          { partition: 2, offset: '2' },
        ]),
      },
    ])
  })
})
