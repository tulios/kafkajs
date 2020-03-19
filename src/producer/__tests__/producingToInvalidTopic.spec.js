const { secureRandom, newLogger, createCluster, createTopic } = require('testHelpers')
const createProducer = require('../index')

describe('Producer > Producing to invalid topics', () => {
  let producer, topicName

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`

    producer = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
    })
    await producer.connect()
    await createTopic({ topic: topicName })
  })

  afterEach(async () => {
    producer && (await producer.disconnect())
  })

  it('it rejects when producing to an invalid topic name, but is able to subsequently produce to a valid topic', async () => {
    producer = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
    })
    await producer.connect()

    const message = { key: `key-${secureRandom()}`, value: `value-${secureRandom()}` }
    const invalidTopicName = `${topicName}-abc)(*&^%`
    await expect(producer.send({ topic: invalidTopicName, messages: [message] })).rejects.toThrow(
      'The request attempted to perform an operation on an invalid topic'
    )

    await producer.send({ topic: topicName, messages: [message] })
  })
})
