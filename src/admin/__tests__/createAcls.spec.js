const createAdmin = require('../index')
// const { KafkaJSProtocolError } = require('../../errors')
// const { createErrorFromCode } = require('../../protocol/error')

const {
  secureRandom,
  createCluster,
  newLogger,
  sslConnectionOpts,
  saslBrokers,
} = require('testHelpers')

const RESOURCE_TYPES = require('../../protocol/resourceTypes')
const OPERATION_TYPES = require('../../protocol/operationsTypes')
const PERMISSION_TYPES = require('../../protocol/permissionTypes')
const RESOURCE_PATTERN_TYPES = require('../../protocol/resourcePatternTypes')

describe('Admin', () => {
  const topicName = `test-topic-${secureRandom()}`
  let admin

  afterEach(async () => {
    await admin.disconnect()
  })

  describe('createAcls', () => {
    test('create new topic and apply acl', async () => {
      const saslConnectionOpts = () =>
        Object.assign(sslConnectionOpts(), {
          port: 9094,
          sasl: {
            mechanism: 'plain',
            username: 'test',
            password: 'testtest',
          },
        })
      admin = createAdmin({
        cluster: createCluster(saslConnectionOpts(), saslBrokers()),
        logger: newLogger(),
      })

      await admin.connect()

      await expect(
        admin.createTopics({
          waitForLeaders: true,
          topics: [{ topic: topicName, numPartitions: 1, replicationFactor: 2 }],
        })
      ).resolves.toEqual(true)

      await expect(
        admin.createAcls({
          acl: [
            {
              resourceType: RESOURCE_TYPES.TOPIC,
              resourceName: topicName,
              resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
              principal: 'User:bob',
              host: '*',
              operation: OPERATION_TYPES.ALL,
              permissionType: PERMISSION_TYPES.DENY,
            },
            {
              resourceType: RESOURCE_TYPES.TOPIC,
              resourceName: topicName,
              resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
              principal: 'User:alice',
              host: '*',
              operation: OPERATION_TYPES.ALL,
              permissionType: PERMISSION_TYPES.ALLOW,
            },
          ],
        })
      ).resolves.toEqual(true)
    })

    test('filter and fetch acl', async () => {
      const saslConnectionOpts = () =>
        Object.assign(sslConnectionOpts(), {
          port: 9094,
          sasl: {
            mechanism: 'plain',
            username: 'test',
            password: 'testtest',
          },
        })
      admin = createAdmin({
        cluster: createCluster(saslConnectionOpts(), saslBrokers()),
        logger: newLogger(),
      })

      await admin.connect()

      await expect(
        admin.describeAcls({
          resourceName: topicName,
          resourceType: RESOURCE_TYPES.TOPIC,
          host: '*',
          permissionType: PERMISSION_TYPES.ALLOW,
          operation: OPERATION_TYPES.ANY,
          resourcePatternTypeFilter: RESOURCE_PATTERN_TYPES.LITERAL,
        })
      ).resolves.toMatchObject({
        resources: [
          {
            resourceType: RESOURCE_TYPES.TOPIC,
            resourceName: topicName,
            resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
            acls: [
              {
                principal: 'User:alice',
                host: '*',
                operation: OPERATION_TYPES.ALL,
                permissionType: PERMISSION_TYPES.ALLOW,
              },
            ],
          },
        ],
      })
    })

    test('Try describe topic using an unauthorized user', async () => {
      const saslConnectionOpts = () =>
        Object.assign(sslConnectionOpts(), {
          port: 9094,
          sasl: {
            mechanism: 'plain',
            username: 'bob',
            password: 'bobbob',
          },
        })
      admin = createAdmin({
        cluster: createCluster(saslConnectionOpts(), saslBrokers()),
        logger: newLogger(),
      })

      await admin.connect()

      await expect(admin.getTopicMetadata({ topics: [topicName] })).rejects.toThrow(
        'Not authorized to access topics: [Topic authorization failed]'
      )
    })

    test('Try describe topic using an authorized user', async () => {
      const saslConnectionOpts = () =>
        Object.assign(sslConnectionOpts(), {
          port: 9094,
          sasl: {
            mechanism: 'plain',
            username: 'alice',
            password: 'alicealice',
          },
        })
      admin = createAdmin({
        cluster: createCluster(saslConnectionOpts(), saslBrokers()),
        logger: newLogger(),
      })

      await admin.connect()

      await expect(admin.getTopicMetadata({ topics: [topicName] })).resolves.toBeTruthy()
    })

    test('Delete ACL', async () => {
      const saslConnectionOpts = () =>
        Object.assign(sslConnectionOpts(), {
          port: 9094,
          sasl: {
            mechanism: 'plain',
            username: 'test',
            password: 'testtest',
          },
        })
      admin = createAdmin({
        cluster: createCluster(saslConnectionOpts(), saslBrokers()),
        logger: newLogger(),
      })

      await admin.connect()

      await expect(
        admin.deleteAcls({
          filters: [
            {
              resourceName: topicName,
              resourceType: RESOURCE_TYPES.TOPIC,
              host: '*',
              permissionType: PERMISSION_TYPES.ALLOW,
              operation: OPERATION_TYPES.ANY,
              resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
            },
          ],
        })
      ).resolves.toMatchObject({
        filterResponses: [
          {
            errorCode: 0,
            errorMessage: null,
            matchingAcls: [
              {
                errorCode: 0,
                errorMessage: null,
                resourceType: RESOURCE_TYPES.TOPIC,
                resourceName: topicName,
                resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
                principal: 'User:alice',
                host: '*',
                operation: OPERATION_TYPES.ALL,
                permissionType: PERMISSION_TYPES.ALLOW,
              },
            ],
          },
        ],
      })

      await expect(
        admin.describeAcls({
          resourceName: topicName,
          resourceType: RESOURCE_TYPES.TOPIC,
          host: '*',
          permissionType: PERMISSION_TYPES.ALLOW,
          operation: OPERATION_TYPES.ANY,
          resourcePatternTypeFilter: RESOURCE_PATTERN_TYPES.LITERAL,
        })
      ).resolves.toMatchObject({ resources: [] })
    })
  })
})
