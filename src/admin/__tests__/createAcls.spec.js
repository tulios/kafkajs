const createAdmin = require('../index')
const createProducer = require('../../producer/index')

const {
  secureRandom,
  createCluster,
  newLogger,
  sslConnectionOpts,
  saslBrokers,
} = require('testHelpers')

const ACL_RESOURCE_TYPES = require('../../protocol/aclResourceTypes')
const ACL_OPERATION_TYPES = require('../../protocol/aclOperationTypes')
const ACL_PERMISSION_TYPES = require('../../protocol/aclPermissionTypes')
const RESOURCE_PATTERN_TYPES = require('../../protocol/resourcePatternTypes')

const createSASLClientForUser = createClient => ({ username, password }) => {
  const saslConnectionOpts = () => {
    return Object.assign(sslConnectionOpts(), {
      port: 9094,
      sasl: {
        mechanism: 'plain',
        username,
        password,
      },
    })
  }

  const client = createClient({
    logger: newLogger(),
    cluster: createCluster(
      {
        ...saslConnectionOpts(),
        metadataMaxAge: 50,
      },
      saslBrokers()
    ),
  })

  return client
}

const createSASLAdminClientForUser = createSASLClientForUser(createAdmin)
const createSASLProducerClientForUser = createSASLClientForUser(createProducer)

describe('Admin', () => {
  let admin

  afterEach(async () => {
    admin && (await admin.disconnect())
  })

  describe('createAcls', () => {
    test('throws an error if the acl array is invalid', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      await expect(admin.createAcls({ acl: 'this-is-not-an-array' })).rejects.toHaveProperty(
        'message',
        'Invalid ACL array this-is-not-an-array'
      )
    })

    test('throws an error if the resource name is invalid', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 123,
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        'Invalid ACL array, the resourceNames have to be a valid string'
      )
    })

    test('throws an error if the principal name is invalid', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 123,
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        'Invalid ACL array, the principals have to be a valid string'
      )
    })

    test('throws an error if the host name is invalid', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: 123,
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        'Invalid ACL array, the hosts have to be a valid string'
      )
    })

    test('throws an error if there are invalid resource types', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: 123,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        `Invalid resource type 123: ${JSON.stringify(ACLEntry)}`
      )
    })

    test('throws an error if there are invalid resource pattern types', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: 123,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        `Invalid resource pattern type 123: ${JSON.stringify(ACLEntry)}`
      )
    })

    test('throws an error if there are invalid permission types', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: 123,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        `Invalid permission type 123: ${JSON.stringify(ACLEntry)}`
      )
    })

    test('throws an error if there are invalid operation types', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: 123,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        `Invalid operation type 123: ${JSON.stringify(ACLEntry)}`
      )
    })

    test('checks topic access', async () => {
      const topicName = `test-topic-${secureRandom()}`

      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })

      await admin.connect()
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

      await admin.disconnect()
      admin = createSASLAdminClientForUser({ username: 'bob', password: 'bobbob' })
      await admin.connect()

      await expect(admin.fetchTopicMetadata({ topics: [topicName] })).rejects.toThrow(
        'Not authorized to access topics: [Topic authorization failed]'
      )

      await admin.disconnect()
      admin = createSASLAdminClientForUser({ username: 'alice', password: 'alicealice' })
      await admin.connect()

      await expect(admin.fetchTopicMetadata({ topics: [topicName] })).resolves.toBeTruthy()
    })

    test('can produce to allowed topic after failing to produce to not-allowed topic', async () => {
      const allowedTopic = `allowed-${secureRandom()}`
      const notAllowedTopic = `disallowed-${secureRandom()}`

      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })

      await admin.connect()
      await admin.createTopics({
        waitForLeaders: true,
        topics: [allowedTopic, notAllowedTopic].map(topic => ({ topic, numPartitions: 1 })),
      })
      await admin.createAcls({
        acl: [
          {
            resourceType: ACL_RESOURCE_TYPES.TOPIC,
            resourceName: notAllowedTopic,
            resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
            principal: 'User:bob',
            host: '*',
            operation: ACL_OPERATION_TYPES.WRITE,
            permissionType: ACL_PERMISSION_TYPES.DENY,
          },
          {
            resourceType: ACL_RESOURCE_TYPES.TOPIC,
            resourceName: allowedTopic,
            resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
            principal: 'User:bob',
            host: '*',
            operation: ACL_OPERATION_TYPES.WRITE,
            permissionType: ACL_PERMISSION_TYPES.ALLOW,
          },
        ],
      })

      await admin.disconnect()
      const producer = createSASLProducerClientForUser({ username: 'bob', password: 'bobbob' })
      await producer.connect()

      await expect(
        producer.send({ topic: allowedTopic, messages: [{ value: 'hello' }] })
      ).resolves.not.toBeUndefined()
      await expect(
        producer.send({ topic: notAllowedTopic, messages: [{ value: 'whoops' }] })
      ).rejects.not.toBeUndefined()
      await expect(
        producer.send({ topic: allowedTopic, messages: [{ value: 'world' }] })
      ).resolves.not.toBeUndefined()

      await producer.disconnect()
    })
  })
})
