const createAdmin = require('../index')

const {
  secureRandom,
  createCluster,
  newLogger,
  saslConnectionOpts,
  saslBrokers,
} = require('testHelpers')

const ACL_RESOURCE_TYPES = require('../../protocol/aclResourceTypes')
const ACL_OPERATION_TYPES = require('../../protocol/aclOperationTypes')
const ACL_PERMISSION_TYPES = require('../../protocol/aclPermissionTypes')
const RESOURCE_PATTERN_TYPES = require('../../protocol/resourcePatternTypes')

describe('Admin', () => {
  let admin

  beforeEach(async () => {
    admin = createAdmin({
      logger: newLogger(),
      cluster: createCluster(
        {
          ...saslConnectionOpts(),
          metadataMaxAge: 50,
        },
        saslBrokers()
      ),
    })

    await admin.connect()
  })

  afterEach(async () => {
    admin && (await admin.disconnect())
  })

  describe('describeAcls', () => {
    test('throws an error if the resource name is invalid', async () => {
      const args = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 123,
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        'Invalid resourceName, the resourceName have to be a valid string'
      )
    })

    test('throws an error if the principal name is invalid', async () => {
      const args = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 123,
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        'Invalid principal, the principal have to be a valid string'
      )
    })

    test('throws an error if the host name is invalid', async () => {
      const args = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: 123,
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        'Invalid host, the host have to be a valid string'
      )
    })

    test('throws an error if there are invalid resource types', async () => {
      const args = {
        resourceType: 123,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        `Invalid resource type ${args.resourceType}`
      )
    })

    test('throws an error if there are invalid resource pattern types', async () => {
      const args = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: 123,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        `Invalid resource pattern filter type ${args.resourcePatternType}`
      )
    })

    test('throws an error if there are invalid permission types', async () => {
      const args = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: 123,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        `Invalid permission type ${args.permissionType}`
      )
    })

    test('throws an error if there are invalid operation types', async () => {
      const args = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: 123,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.describeAcls(args)).rejects.toHaveProperty(
        'message',
        `Invalid operation type ${args.operation}`
      )
    })

    test('creates and queries acl', async () => {
      const topicName = `test-topic-${secureRandom()}`

      await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: topicName, numPartitions: 1, replicationFactor: 2 }],
      })

      await expect(
        admin.createAcls({
          acl: [
            {
              resourceType: ACL_RESOURCE_TYPES.TOPIC,
              resourceName: topicName,
              resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
              principal: 'User:bob',
              host: '*',
              operation: ACL_OPERATION_TYPES.ALL,
              permissionType: ACL_PERMISSION_TYPES.DENY,
            },
            {
              resourceType: ACL_RESOURCE_TYPES.TOPIC,
              resourceName: topicName,
              resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
              principal: 'User:alice',
              host: '*',
              operation: ACL_OPERATION_TYPES.ALL,
              permissionType: ACL_PERMISSION_TYPES.ALLOW,
            },
          ],
        })
      ).resolves.toEqual(true)

      await expect(
        admin.describeAcls({
          resourceName: topicName,
          resourceType: ACL_RESOURCE_TYPES.TOPIC,
          host: '*',
          permissionType: ACL_PERMISSION_TYPES.ALLOW,
          operation: ACL_OPERATION_TYPES.ANY,
          resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        })
      ).resolves.toMatchObject({
        resources: [
          {
            resourceType: ACL_RESOURCE_TYPES.TOPIC,
            resourceName: topicName,
            resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
            acls: [
              {
                principal: 'User:alice',
                host: '*',
                operation: ACL_OPERATION_TYPES.ALL,
                permissionType: ACL_PERMISSION_TYPES.ALLOW,
              },
            ],
          },
        ],
      })
    })
  })
})
