const { secureRandom, newLogger, createCluster, createTopic, deleteTopic } = require('testHelpers')
const createProducer = require('../index')

describe('Producer', () => {
  let producer, topicName, cluster

  const newMessage = () => ({ key: `key-${secureRandom()}`, value: `value-${secureRandom()}` })

  beforeEach(async () => {
    cluster = createCluster()
    topicName = `test-topic-${secureRandom()}`
    producer = createProducer({
      cluster,
      logger: newLogger(),
    })

    await producer.connect()
    await createTopic({ topic: topicName, partitions: 1 })
  })

  afterEach(async () => {
    producer && (await producer.disconnect())
  })

  describe('producing to a deleted topic', () => {
    test('refreshes metadata and ', async () => {
      await producer.send({ topic: topicName, messages: [newMessage()] })

      await deleteTopic(topicName)

      const refreshMetadataSpy = jest.spyOn(cluster, 'refreshMetadata')

      await producer
        .send({ topic: topicName, messages: [newMessage()] })
        .catch(e => console.error(e))

      expect(refreshMetadataSpy).toHaveBeenCalledTimes(1)
    })
  })
})
