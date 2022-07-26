const { MemberMetadata, MemberAssignment } = require('../assignerProtocol')

const createConsumer = require('../index')

const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

describe('Consumer > assignerProtocol > integration', () => {
  let topicName, groupId, cluster1, cluster2, consumer1, consumer2

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

    cluster1 = createCluster()
    cluster2 = createCluster()
  })

  afterEach(async () => {
    consumer1 && (await consumer1.disconnect())
    consumer2 && (await consumer2.disconnect())
  })

  test('provides member user data', async () => {
    const assignmentMemberUserData = {}
    function createAssigner(id) {
      return () => ({
        name: 'TestAssigner',
        version: 1,
        protocol({ topics }) {
          // Encode the id as user data
          return {
            name: this.name,
            metadata: MemberMetadata.encode({
              version: this.version,
              topics,
              userData: Buffer.from(id),
            }),
          }
        },
        async assign({ members }) {
          // Dummy assignment: just assign nothing to each member, but keep the
          // user data for each member for checking it later.
          return members.map(({ memberId, memberMetadata }) => {
            const decodedMetadata = MemberMetadata.decode(memberMetadata)
            assignmentMemberUserData[memberId] = decodedMetadata.userData.toString('utf8')

            return {
              memberId,
              memberAssignment: MemberAssignment.encode({
                version: this.version,
                assignment: [],
              }),
            }
          })
        },
      })
    }
    // Connect the two consumers to their respective clusters
    consumer1 = createConsumer({
      cluster: cluster1,
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
      partitionAssigners: [createAssigner('consumer1')],
    })

    consumer2 = createConsumer({
      cluster: cluster2,
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
      partitionAssigners: [createAssigner('consumer2')],
    })

    // Wait until the consumers are connected
    await Promise.all([consumer1.connect(), consumer2.connect()])

    // Subscribe both consumers to the same topic, and start the consumer groups
    consumer1.subscribe({ topic: topicName })
    consumer1.run({ eachMessage: () => {} })
    consumer2.subscribe({ topic: topicName })
    consumer2.run({ eachMessage: () => {} })

    await Promise.all([
      waitForConsumerToJoinGroup(consumer1),
      waitForConsumerToJoinGroup(consumer2),
    ])

    // Check that we now have seen for each member a matching entry
    expect(Object.values(assignmentMemberUserData).sort()).toEqual(['consumer1', 'consumer2'])
  })
})
