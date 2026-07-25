const { secureRandom, newLogger, createCluster, createTopic } = require('testHelpers')
const createConsumer = require('../index')

describe('Consumer > clusterId', () => {
  let consumer, groupId

  beforeEach(async () => {
    groupId = `consumer-group-id-${secureRandom()}`
    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
  })

  test('returns null before connecting', () => {
    expect(consumer.clusterId()).toBeNull()
  })

  test('returns a non-empty string after connecting', async () => {
    await consumer.connect()
    const id = consumer.clusterId()
    expect(id).toBeTruthy()
    expect(typeof id).toBe('string')
  })
})
