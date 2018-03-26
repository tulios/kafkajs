const createConsumer = require('../index')

const { secureRandom, createCluster, newLogger } = require('testHelpers')

describe('Consumer', () => {
  let groupId, cluster, consumer

  beforeEach(async () => {
    groupId = `consumer-group-id-${secureRandom()}`

    cluster = createCluster()
    consumer = createConsumer({
      cluster,
      groupId,
      maxWaitTimeInMs: 1,
      maxBytesPerPartition: 180,
      logger: newLogger(),
    })
  })

  describe('when subscribe', () => {
    it('throws an error if the topic is invalid', async () => {
      await expect(consumer.subscribe({ topic: null })).rejects.toHaveProperty(
        'message',
        'Invalid topic null'
      )
    })
  })
})
