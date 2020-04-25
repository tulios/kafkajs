const createAdmin = require('../index')

const {
  secureRandom,
  createCluster,
  newLogger,
  saslConnectionOpts,
  saslBrokers,
} = require('testHelpers')

const RESOURCE_TYPES = require('../../protocol/resourceTypes')
const OPERATION_TYPES = require('../../protocol/operationsTypes')
const PERMISSION_TYPES = require('../../protocol/permissionTypes')
const RESOURCE_PATTERN_TYPES = require('../../protocol/resourcePatternTypes')

describe('Admin', () => {
  let admin

  afterEach(async () => {
    await admin.disconnect()
  })

  describe('describeAcls', () => {
    test('throws an error if the resource name is invalid', async () => {
      admin = createAdmin({
        cluster: createCluster(saslConnectionOpts(), saslBrokers()),
        logger: newLogger(),
      })
      await admin.connect()

      const args = {
        resourceType: RESOURCE_TYPES.TOPIC,
        resourceName: 123,
        resourcePatternTypeFilter: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: OPERATION_TYPES.ALL,
        permissionType: PERMISSION_TYPES.DENY,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        'Invalid resourceName, the resourceName have to be a valid string'
      )
    })

    test('throws an error if the principal name is invalid', async () => {
      admin = createAdmin({
        cluster: createCluster(saslConnectionOpts(), saslBrokers()),
        logger: newLogger(),
      })
      await admin.connect()

      const args = {
        resourceType: RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternTypeFilter: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 123,
        host: '*',
        operation: OPERATION_TYPES.ALL,
        permissionType: PERMISSION_TYPES.DENY,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        'Invalid principal, the principal have to be a valid string'
      )
    })

    test('throws an error if the host name is invalid', async () => {
      admin = createAdmin({
        cluster: createCluster(saslConnectionOpts(), saslBrokers()),
        logger: newLogger(),
      })
      await admin.connect()

      const args = {
        resourceType: RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternTypeFilter: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: 123,
        operation: OPERATION_TYPES.ALL,
        permissionType: PERMISSION_TYPES.DENY,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        'Invalid host, the host have to be a valid string'
      )
    })

    test('throws an error if there are invalid resource types', async () => {
      admin = createAdmin({
        cluster: createCluster(saslConnectionOpts(), saslBrokers()),
        logger: newLogger(),
      })
      await admin.connect()

      const args = {
        resourceType: 123,
        resourceName: 'foo',
        resourcePatternTypeFilter: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: OPERATION_TYPES.ALL,
        permissionType: PERMISSION_TYPES.DENY,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        `Invalid resource type ${args.resourceType}`
      )
    })

    test('throws an error if there are invalid resource pattern types', async () => {
      admin = createAdmin({
        cluster: createCluster(saslConnectionOpts(), saslBrokers()),
        logger: newLogger(),
      })
      await admin.connect()

      const args = {
        resourceType: RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternTypeFilter: 123,
        principal: 'User:foo',
        host: '*',
        operation: OPERATION_TYPES.ALL,
        permissionType: PERMISSION_TYPES.DENY,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        `Invalid resource pattern filter type ${args.resourcePatternTypeFilter}`
      )
    })

    test('throws an error if there are invalid permission types', async () => {
      admin = createAdmin({
        cluster: createCluster(saslConnectionOpts(), saslBrokers()),
        logger: newLogger(),
      })
      await admin.connect()

      const args = {
        resourceType: RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternTypeFilter: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: OPERATION_TYPES.ALL,
        permissionType: 123,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        `Invalid permission type ${args.permissionType}`
      )
    })

    test('throws an error if there are invalid operation types', async () => {
      admin = createAdmin({
        cluster: createCluster(saslConnectionOpts(), saslBrokers()),
        logger: newLogger(),
      })
      await admin.connect()

      const args = {
        resourceType: RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternTypeFilter: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: 123,
        permissionType: PERMISSION_TYPES.DENY,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        `Invalid operation type ${args.operation}`
      )
    })

    test('creates and queries acl', async () => {
      const topicName = `test-topic-${secureRandom()}`

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
  })
})
