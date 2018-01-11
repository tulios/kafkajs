const { createCluster, secureRandom, createTopic } = require('../../testHelpers')

describe('Cluster > fetchGroupOffset', () => {
  let cluster, topic, groupId

  beforeEach(async () => {
    cluster = createCluster()
    await cluster.connect()
    await cluster.refreshMetadata()
    topic = `test-topic-${secureRandom()}`
    groupId = `test-group-${secureRandom()}`

    createTopic({ topic, partitions: 3 })
  })

  afterEach(async () => {
    cluster && (await cluster.disconnect())
  })

  test('return group offset', async () => {
    const topicsWithPartitions = [
      {
        topic,
        partitions: [{ partition: 0 }, { partition: 1 }, { partition: 2 }],
      },
    ]

    const offsets = await cluster.fetchGroupOffset({ groupId, topicsWithPartitions })
    expect(offsets).toEqual([
      {
        topic,
        partitions: [
          { errorCode: 0, metadata: '', offset: '-1', partition: 0 },
          { errorCode: 0, metadata: '', offset: '-1', partition: 1 },
          { errorCode: 0, metadata: '', offset: '-1', partition: 2 },
        ],
      },
    ])
  })
})
