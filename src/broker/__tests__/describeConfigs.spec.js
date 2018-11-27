const { createConnection, connectionOpts, secureRandom, newLogger } = require('testHelpers')
const RESOURCE_TYPES = require('../../protocol/resourceTypes')
const Broker = require('../index')

const sortByConfigName = array => array.sort((a, b) => a.configName.localeCompare(b.configName))

describe('Broker > describeConfigs', () => {
  let seedBroker, broker

  beforeEach(async () => {
    seedBroker = new Broker({
      connection: createConnection(connectionOpts()),
      logger: newLogger(),
    })
    await seedBroker.connect()

    const metadata = await seedBroker.metadata()
    const newBrokerData = metadata.brokers.find(b => b.nodeId === metadata.controllerId)

    broker = new Broker({
      connection: createConnection(newBrokerData),
      logger: newLogger(),
    })
  })

  afterEach(async () => {
    seedBroker && (await seedBroker.disconnect())
    broker && (await broker.disconnect())
  })

  test('request', async () => {
    await broker.connect()
    const topicName1 = `test-topic-${secureRandom()}`
    const topicName2 = `test-topic-${secureRandom()}`

    await broker.createTopics({
      topics: [{ topic: topicName1 }, { topic: topicName2 }],
    })

    const response = await broker.describeConfigs({
      resources: [
        {
          type: RESOURCE_TYPES.TOPIC,
          name: topicName1,
          configNames: ['compression.type', 'retention.ms'],
        },
      ],
    })

    expect(response).toEqual({
      resources: [
        {
          configEntries: [
            {
              configName: 'compression.type',
              configValue: 'producer',
              isDefault: true,
              isSensitive: false,
              readOnly: false,
            },
            {
              configName: 'retention.ms',
              configValue: '604800000',
              isDefault: true,
              isSensitive: false,
              readOnly: false,
            },
          ],
          errorCode: 0,
          errorMessage: null,
          resourceName: topicName1,
          resourceType: RESOURCE_TYPES.TOPIC,
        },
      ],
      throttleTime: 0,
    })
  })

  describe('request without config names', () => {
    test('returns all config entries', async () => {
      await broker.connect()
      const topicName1 = `test-topic-${secureRandom()}`
      const topicName2 = `test-topic-${secureRandom()}`

      await broker.createTopics({
        topics: [{ topic: topicName1 }, { topic: topicName2 }],
      })

      const response = await broker.describeConfigs({
        resources: [
          {
            type: RESOURCE_TYPES.TOPIC,
            name: topicName1,
            configNames: [],
          },
        ],
      })

      const expectedConfigEntries = sortByConfigName([
        {
          configName: 'compression.type',
          configValue: 'producer',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'message.format.version',
          configValue: expect.stringMatching(/^(0\.11\.0-IV2|1\.1-IV0)$/),
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'file.delete.delay.ms',
          configValue: '60000',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'leader.replication.throttled.replicas',
          configValue: '',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'max.message.bytes',
          configValue: '1000012',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'min.compaction.lag.ms',
          configValue: '0',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'message.timestamp.type',
          configValue: 'CreateTime',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'min.insync.replicas',
          configValue: '1',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'segment.jitter.ms',
          configValue: '0',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'preallocate',
          configValue: 'false',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'index.interval.bytes',
          configValue: '4096',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'min.cleanable.dirty.ratio',
          configValue: '0.5',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'unclean.leader.election.enable',
          configValue: 'false',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'retention.bytes',
          configValue: '-1',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'delete.retention.ms',
          configValue: '86400000',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'cleanup.policy',
          configValue: 'delete',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'flush.ms',
          configValue: '9223372036854775807',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'follower.replication.throttled.replicas',
          configValue: '',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'segment.bytes',
          configValue: '1073741824',
          isDefault: expect.any(Boolean),
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'retention.ms',
          configValue: '604800000',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'segment.ms',
          configValue: '604800000',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'message.timestamp.difference.max.ms',
          configValue: '9223372036854775807',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'flush.messages',
          configValue: '9223372036854775807',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
        {
          configName: 'segment.index.bytes',
          configValue: '10485760',
          isDefault: true,
          isSensitive: false,
          readOnly: false,
        },
      ])

      expect(response).toEqual(
        expect.objectContaining({
          resources: [
            expect.objectContaining({
              configEntries: expect.any(Array),
              errorCode: 0,
              errorMessage: null,
              resourceName: topicName1,
              resourceType: RESOURCE_TYPES.TOPIC,
            }),
          ],
          throttleTime: 0,
        })
      )

      expect(sortByConfigName(response.resources[0].configEntries)).toEqual(
        expect.arrayContaining(expectedConfigEntries)
      )
    })
  })
})
