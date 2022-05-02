const createAdmin = require('../index')
const { KafkaJSProtocolError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')

const { secureRandom, createCluster, newLogger, createTopic } = require('testHelpers')
const CONFIG_RESOURCE_TYPES = require('../../protocol/configResourceTypes')
const NOT_CONTROLLER = 41

describe('Admin', () => {
  let topicName, admin

  const getConfigEntries = response =>
    response.resources.find(r => r.resourceType === CONFIG_RESOURCE_TYPES.TOPIC).configEntries

  const getConfigValue = (configEntries, name) =>
    configEntries.find(c => c.configName === name).configValue

  beforeEach(() => {
    topicName = `test-topic-${secureRandom()}`
  })

  afterEach(async () => {
    admin && (await admin.disconnect())
  })

  describe('describeConfigs', () => {
    test('throws an error if the resources array is invalid', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.describeConfigs({ resources: null })).rejects.toHaveProperty(
        'message',
        'Invalid resources array null'
      )

      await expect(
        admin.describeConfigs({ resources: 'this-is-not-an-array' })
      ).rejects.toHaveProperty('message', 'Invalid resources array this-is-not-an-array')
    })

    test('throws an error if the resources array is empty', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      await expect(admin.describeConfigs({ resources: [] })).rejects.toHaveProperty(
        'message',
        'Resources array cannot be empty'
      )
    })

    test('throws an error if there are invalid resource types', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      const resources = [{ type: CONFIG_RESOURCE_TYPES.TOPIC }, { type: 1999 }]
      await expect(admin.describeConfigs({ resources })).rejects.toHaveProperty(
        'message',
        'Invalid resource type 1999: {"type":1999}'
      )
    })

    test('throws an error if there are blank resource names', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      const resources = [
        { type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'abc' },
        { type: CONFIG_RESOURCE_TYPES.TOPIC, name: null },
      ]
      await expect(admin.describeConfigs({ resources })).rejects.toHaveProperty(
        'message',
        'Invalid resource name null: {"type":2,"name":null}'
      )
    })

    test('throws an error if there are invalid resource names', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      const resources = [
        { type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'abc' },
        { type: CONFIG_RESOURCE_TYPES.TOPIC, name: 123 },
      ]
      await expect(admin.describeConfigs({ resources })).rejects.toHaveProperty(
        'message',
        'Invalid resource name 123: {"type":2,"name":123}'
      )
    })

    test('throws an error if there are invalid resource configNames', async () => {
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })
      const resources = [
        { type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'abc', configNames: [] },
        { type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'def', configNames: 123 },
      ]
      await expect(admin.describeConfigs({ resources })).rejects.toHaveProperty(
        'message',
        'Invalid resource configNames 123: {"type":2,"name":"def","configNames":123}'
      )
    })

    test('describe all configs', async () => {
      await createTopic({ topic: topicName })
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()
      const response = await admin.describeConfigs({
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: topicName }],
      })

      expect(getConfigEntries(response).length).toBeGreaterThan(1)
    })

    test('describe selected configs', async () => {
      await createTopic({ topic: topicName })
      admin = createAdmin({ cluster: createCluster(), logger: newLogger() })

      await admin.connect()
      const response = await admin.describeConfigs({
        resources: [
          {
            type: CONFIG_RESOURCE_TYPES.TOPIC,
            name: topicName,
            configNames: ['cleanup.policy'],
          },
        ],
      })

      const configEntries = getConfigEntries(response)
      expect(configEntries.length).toEqual(1)
      expect(getConfigValue(configEntries, 'cleanup.policy')).toEqual('delete')
    })

    test('retries if the controller has moved', async () => {
      const cluster = createCluster()
      const brokerResponse = { resources: [true] }
      const broker = { describeConfigs: jest.fn(() => brokerResponse) }

      cluster.refreshMetadata = jest.fn()
      cluster.findControllerBroker = jest
        .fn()
        .mockImplementationOnce(() => {
          throw new KafkaJSProtocolError(createErrorFromCode(NOT_CONTROLLER))
        })
        .mockImplementationOnce(() => broker)

      admin = createAdmin({ cluster, logger: newLogger() })
      await expect(
        admin.describeConfigs({
          resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: topicName }],
        })
      ).resolves.toEqual(brokerResponse)

      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2)
      expect(cluster.findControllerBroker).toHaveBeenCalledTimes(2)
      expect(broker.describeConfigs).toHaveBeenCalledTimes(1)
    })

    test('describe broker configs', async () => {
      await createTopic({ topic: topicName })

      const cluster = createCluster()
      admin = createAdmin({ cluster, logger: newLogger() })
      await admin.connect()

      const metadata = await cluster.brokerPool.seedBroker.metadata()
      const brokers = metadata.brokers
      const brokerToDescribeConfig = brokers[1].nodeId.toString()

      const resources = [
        {
          type: CONFIG_RESOURCE_TYPES.TOPIC,
          name: topicName,
          configNames: ['cleanup.policy'],
        },
        {
          type: CONFIG_RESOURCE_TYPES.BROKER,
          name: brokerToDescribeConfig,
        },
      ]

      const response = await admin.describeConfigs({ resources })
      expect(response.resources.length).toEqual(2)
    })
  })
})
