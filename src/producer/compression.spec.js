const createProducer = require('./index')
const Compression = require('../protocol/message/compression')
const { KafkaJSNotImplemented } = require('../errors')

const {
  secureRandom,
  createTopic,
  createModPartitioner,
  connectionOpts,
  newLogger,
  createCluster,
} = require('testHelpers')

describe('Producer', () => {
  let topicName, cluster, producer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    await createTopic({ topic: topicName })

    cluster = createCluster({ ...connectionOpts(), createPartitioner: createModPartitioner })
    producer = createProducer({ cluster, logger: newLogger() })
    await producer.connect()
  })

  afterEach(async () => {
    producer && (await producer.disconnect())
  })

  const codecsUsingExternalLibraries = [
    { name: 'snappy', codec: Compression.Types.Snappy },
    { name: 'lz4', codec: Compression.Types.LZ4 },
    { name: 'zstd', codec: Compression.Types.ZSTD },
  ]

  for (const entry of codecsUsingExternalLibraries) {
    describe(`${entry.name} compression not configured`, () => {
      it('throws an error', async () => {
        await expect(
          producer.send({
            topic: topicName,
            compression: entry.codec,
            messages: [{ key: secureRandom(), value: secureRandom() }],
          })
        ).rejects.toThrow(KafkaJSNotImplemented)
      })
    })
  }
})
