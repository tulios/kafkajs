const createAdmin = require('../index')

const { secureRandom, createCluster, newLogger } = require('testHelpers')

describe('Admin', () => {
  let cluster, admin, existingTopicNames

  beforeEach(async () => {
    cluster = createCluster()
    admin = createAdmin({ cluster: cluster, logger: newLogger() })
    existingTopicNames = [
      `test-topic-${secureRandom()}`,
      `test-topic-${secureRandom()}`,
      `test-topic-${secureRandom()}`,
    ]
    const topics = existingTopicNames.map(n => ({
      topic: n,
    }))
    await admin.createTopics({ topics })
  })

  afterEach(async () => {
    if (admin) {
      await admin.deleteTopics({ topics: existingTopicNames })
      await admin.disconnect()
    }
  })

  describe('listTopics', () => {
    test('list topics', async () => {
      const listTopicsResponse = await admin.listTopics()
      expect(listTopicsResponse).toEqual(expect.arrayContaining(existingTopicNames))
    })
  })
})
