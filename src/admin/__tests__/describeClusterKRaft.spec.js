const createAdmin = require('../index')
const { createCluster, newLogger, testIfKafkaAtLeast_4_0_0 } = require('testHelpers')

describe('Admin > describeCluster (KRaft)', () => {
  let admin

  afterEach(async () => {
    admin && (await admin.disconnect())
  })

  testIfKafkaAtLeast_4_0_0('retrieves cluster metadata from KRaft-mode brokers', async () => {
    const cluster = createCluster()
    admin = createAdmin({ cluster, logger: newLogger() })

    await admin.connect()
    const { brokers, clusterId, controller } = await admin.describeCluster()

    expect(brokers).toHaveLength(3)
    expect(brokers).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          nodeId: expect.any(Number),
          host: expect.any(String),
          port: expect.any(Number),
        }),
      ])
    )
    expect(clusterId).toEqual(expect.any(String))
    expect(brokers.map(({ nodeId }) => nodeId)).toContain(controller)
  })

  testIfKafkaAtLeast_4_0_0('lists topics on KRaft cluster', async () => {
    const cluster = createCluster()
    admin = createAdmin({ cluster, logger: newLogger() })

    await admin.connect()
    const topics = await admin.listTopics()
    expect(Array.isArray(topics)).toBe(true)
  })
})
