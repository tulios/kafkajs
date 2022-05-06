const createAdmin = require('../index')
const createProducer = require('../../producer')
const createConsumer = require('../../consumer')
const {
  secureRandom,
  createCluster,
  newLogger,
  createTopic,
  createModPartitioner,
  waitForConsumerToJoinGroup,
  generateMessages,
  testIfKafkaAtLeast_0_11,
} = require('testHelpers')
const { KafkaJSNonRetriableError } = require('../../errors')

describe('Admin', () => {
  let admin, cluster, groupId, logger, topicName, anotherTopicName, yetAnotherTopicName

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    anotherTopicName = `another-topic-${secureRandom()}`
    yetAnotherTopicName = `yet-another-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await Promise.all(
      [topicName, anotherTopicName, yetAnotherTopicName].map(topic =>
        createTopic({ topic, numPartitions: 1 })
      )
    )

    logger = newLogger()
    cluster = createCluster()
    admin = createAdmin({ cluster, logger })

    await admin.connect()
  })

  afterEach(async () => {
    admin && (await admin.disconnect())
  })

  describe('fetchOffsets', () => {
    test('throws an error if the groupId is invalid', async () => {
      await expect(admin.fetchOffsets({ groupId: null })).rejects.toThrow(
        KafkaJSNonRetriableError,
        'Invalid groupId null'
      )
    })

    test('throws an error if the topics argument is not a valid list', async () => {
      await expect(admin.fetchOffsets({ groupId: 'groupId', topics: topicName })).rejects.toThrow(
        KafkaJSNonRetriableError,
        'Expected topic or topics array to be set'
      )
    })

    test('returns unresolved consumer group offsets', async () => {
      const offsets = await admin.fetchOffsets({
        groupId,
        topics: [topicName],
      })

      expect(offsets).toEqual([
        { topic: topicName, partitions: [{ partition: 0, offset: '-1', metadata: null }] },
      ])
    })

    test('returns the current consumer group offset', async () => {
      await admin.setOffsets({
        groupId,
        topic: topicName,
        partitions: [{ partition: 0, offset: 13 }],
      })

      const offsets = await admin.fetchOffsets({
        groupId,
        topics: [topicName],
      })

      expect(offsets).toEqual([
        { topic: topicName, partitions: [{ partition: 0, offset: '13', metadata: null }] },
      ])
    })

    test('returns consumer group offsets for all topics', async () => {
      await admin.setOffsets({
        groupId,
        topic: topicName,
        partitions: [{ partition: 0, offset: 13 }],
      })
      await admin.setOffsets({
        groupId,
        topic: anotherTopicName,
        partitions: [{ partition: 0, offset: 23 }],
      })
      await admin.setOffsets({
        groupId,
        topic: yetAnotherTopicName,
        partitions: [{ partition: 0, offset: 42 }],
      })

      const offsets = await admin.fetchOffsets({
        groupId,
      })

      expect(offsets).toIncludeSameMembers([
        {
          topic: yetAnotherTopicName,
          partitions: [{ partition: 0, offset: '42', metadata: null }],
        },
        { topic: anotherTopicName, partitions: [{ partition: 0, offset: '23', metadata: null }] },
        { topic: topicName, partitions: [{ partition: 0, offset: '13', metadata: null }] },
      ])
    })

    test('returns consumer group offsets for list of topics', async () => {
      await admin.setOffsets({
        groupId,
        topic: topicName,
        partitions: [{ partition: 0, offset: 13 }],
      })
      await admin.setOffsets({
        groupId,
        topic: anotherTopicName,
        partitions: [{ partition: 0, offset: 42 }],
      })

      const offsets = await admin.fetchOffsets({
        groupId,
        topics: [topicName, anotherTopicName],
      })

      // There's no guarantee for the order of topics so we compare sets to avoid flaky tests.
      expect(offsets).toIncludeSameMembers([
        { topic: anotherTopicName, partitions: [{ partition: 0, offset: '42', metadata: null }] },
        { topic: topicName, partitions: [{ partition: 0, offset: '13', metadata: null }] },
      ])
    })

    describe('when used with the resolvedOffsets option', () => {
      let producer, consumer

      beforeEach(async done => {
        producer = createProducer({
          cluster,
          createPartitioner: createModPartitioner,
          logger,
        })
        await producer.connect()

        consumer = createConsumer({
          cluster,
          groupId,
          maxWaitTimeInMs: 100,
          logger,
        })

        await consumer.connect()
        await consumer.subscribe({ topic: topicName, fromBeginning: true })
        consumer.run({ eachMessage: () => {} })
        await waitForConsumerToJoinGroup(consumer)

        consumer.on(consumer.events.END_BATCH_PROCESS, async () => {
          // stop the consumer after the first batch, so only 5 are committed
          await consumer.stop()
          // send batch #2
          await producer.send({
            acks: 1,
            topic: topicName,
            messages: generateMessages({ number: 5 }),
          })
          done()
        })

        // send batch #1
        await producer.send({
          acks: 1,
          topic: topicName,
          messages: generateMessages({ number: 5 }),
        })
      })

      afterEach(async () => {
        producer && (await producer.disconnect())
        consumer && (await consumer.disconnect())
      })

      test('no reset: returns latest *committed* consumer offsets', async () => {
        const offsetsBeforeResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
        })
        const offsetsUponResolving = await admin.fetchOffsets({
          groupId,
          topics: [topicName],
          resolveOffsets: true,
        })
        const offsetsAfterResolving = await admin.fetchOffsets({
          groupId,
          topics: [topicName],
        })

        expect(offsetsBeforeResolving).toEqual([
          { topic: topicName, partitions: [{ partition: 0, offset: '5', metadata: null }] },
        ])
        expect(offsetsUponResolving).toEqual([
          { topic: topicName, partitions: [{ partition: 0, offset: '5', metadata: null }] },
        ])
        expect(offsetsAfterResolving).toEqual([
          { topic: topicName, partitions: [{ partition: 0, offset: '5', metadata: null }] },
        ])
      })

      test('reset to latest: returns latest *topic* offsets after resolving', async () => {
        await admin.resetOffsets({ groupId, topic: topicName })

        const offsetsBeforeResolving = await admin.fetchOffsets({
          groupId,
          topics: [topicName],
        })
        const offsetsUponResolving = await admin.fetchOffsets({
          groupId,
          topics: [topicName],
          resolveOffsets: true,
        })
        const offsetsAfterResolving = await admin.fetchOffsets({
          groupId,
          topics: [topicName],
        })

        expect(offsetsBeforeResolving).toEqual([
          { topic: topicName, partitions: [{ partition: 0, offset: '-1', metadata: null }] },
        ])
        expect(offsetsUponResolving).toEqual([
          { topic: topicName, partitions: [{ partition: 0, offset: '10', metadata: null }] },
        ])
        expect(offsetsAfterResolving).toEqual([
          { topic: topicName, partitions: [{ partition: 0, offset: '10', metadata: null }] },
        ])
      })

      test('reset to earliest: returns earliest *topic* offsets after resolving', async () => {
        await admin.resetOffsets({ groupId, topic: topicName, earliest: true })

        const offsetsBeforeResolving = await admin.fetchOffsets({
          groupId,
          topics: [topicName],
        })
        const offsetsUponResolving = await admin.fetchOffsets({
          groupId,
          topics: [topicName],
          resolveOffsets: true,
        })
        const offsetsAfterResolving = await admin.fetchOffsets({
          groupId,
          topics: [topicName],
        })

        expect(offsetsBeforeResolving).toEqual([
          { topic: topicName, partitions: [{ partition: 0, offset: '-2', metadata: null }] },
        ])
        expect(offsetsUponResolving).toEqual([
          { topic: topicName, partitions: [{ partition: 0, offset: '0', metadata: null }] },
        ])
        expect(offsetsAfterResolving).toEqual([
          { topic: topicName, partitions: [{ partition: 0, offset: '0', metadata: null }] },
        ])
      })

      testIfKafkaAtLeast_0_11(
        'will return the correct earliest offset when it is greater than 0',
        async () => {
          // simulate earliest offset = 7, by deleting first 7 messages from the topic
          const messagesToDelete = [
            {
              partition: 0,
              offset: '7',
            },
          ]

          await admin.deleteTopicRecords({ topic: topicName, partitions: messagesToDelete })
          await admin.resetOffsets({ groupId, topic: topicName, earliest: true })

          const offsetsBeforeResolving = await admin.fetchOffsets({
            groupId,
            topics: [topicName],
          })
          const offsetsUponResolving = await admin.fetchOffsets({
            groupId,
            topics: [topicName],
            resolveOffsets: true,
          })
          const offsetsAfterResolving = await admin.fetchOffsets({
            groupId,
            topics: [topicName],
          })

          expect(offsetsBeforeResolving).toEqual([
            { topic: topicName, partitions: [{ partition: 0, offset: '-2', metadata: null }] },
          ])
          expect(offsetsUponResolving).toEqual([
            { topic: topicName, partitions: [{ partition: 0, offset: '7', metadata: null }] },
          ])
          expect(offsetsAfterResolving).toEqual([
            { topic: topicName, partitions: [{ partition: 0, offset: '7', metadata: null }] },
          ])
        }
      )
    })
  })
})
