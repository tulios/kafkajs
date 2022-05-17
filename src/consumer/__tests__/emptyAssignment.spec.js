const { MemberMetadata, MemberAssignment } = require('../assignerProtocol')

const createConsumer = require('../index')

const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitForConsumerToJoinGroup,
  waitForNextEvent,
} = require('testHelpers')

describe('Consumer', () => {
  let topic, groupId, consumer1, consumer2

  beforeEach(async () => {
    topic = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topic, partitions: 1 })
  })

  afterEach(async () => {
    consumer1 && (await consumer1.disconnect())
    consumer2 && (await consumer2.disconnect())
  })

  test('remains in the group without receiving any assignment', async () => {
    // Assigns all topic-partitions to the first member.
    const UnbalancedAssigner = ({ cluster }) => ({
      name: 'UnbalancedAssigner',
      version: 1,
      async assign({ members, topics, userData }) {
        const sortedMembers = members.map(({ memberId }) => memberId).sort()
        const firstMember = sortedMembers[0]
        const assignment = {
          [firstMember]: {},
        }

        topics.forEach(topic => {
          const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
          const partitions = partitionMetadata.map(m => m.partitionId)
          assignment[firstMember][topic] = partitions
        })

        return Object.keys(assignment).map(memberId => ({
          memberId,
          memberAssignment: MemberAssignment.encode({
            version: this.version,
            assignment: assignment[memberId],
            userData,
          }),
        }))
      },
      protocol({ topics, userData }) {
        return {
          name: this.name,
          metadata: MemberMetadata.encode({
            version: this.version,
            topics,
            userData,
          }),
        }
      },
    })

    consumer1 = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
      partitionAssigners: [UnbalancedAssigner],
    })

    consumer2 = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 1,
      logger: newLogger(),
      partitionAssigners: [UnbalancedAssigner],
    })

    await Promise.all([consumer1.connect(), consumer2.connect()])

    consumer1.subscribe({ topic })
    consumer2.subscribe({ topic })

    consumer1.run({ eachMessage: () => {} })
    consumer2.run({ eachMessage: () => {} })

    // Ensure that both consumers manage to join
    const groupJoinEvents = await Promise.all([
      waitForConsumerToJoinGroup(consumer1),
      waitForConsumerToJoinGroup(consumer2),
    ])

    const emptyAssignments = groupJoinEvents.filter(
      ({ payload }) => Object.entries(payload.memberAssignment).length === 0
    )
    expect(emptyAssignments).toHaveLength(1)

    await Promise.all(
      [consumer1, consumer2].map(consumer => waitForNextEvent(consumer, consumer.events.FETCH))
    )

    // Both consumers should continue to heartbeat even without receiving any assignments
    await Promise.all(
      [consumer1, consumer2].map(consumer => waitForNextEvent(consumer, consumer.events.HEARTBEAT))
    )
  })
})
