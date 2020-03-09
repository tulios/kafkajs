const createAdmin = require('../index')

const { createCluster, newLogger } = require('testHelpers')

describe('Admin', () => {
  let admin

  afterEach(async () => {
    admin && (await admin.disconnect())
  })

  describe('describeCluster', () => {
    test('retrieves metadata for all brokers in the cluster', async () => {
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
  })
})
