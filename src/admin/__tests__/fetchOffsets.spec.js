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
const Encoder = require('../../protocol/encoder')
const Decoder = require('../../protocol/decoder')

describe('Admin', () => {
  let admin, cluster, groupId, logger, topicName

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`

    await createTopic({ topic: topicName })

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
      await expect(admin.fetchOffsets({ groupId: null })).rejects.toHaveProperty(
        'message',
        'Invalid groupId null'
      )
    })

    test('throws an error if the topic name is not a valid string', async () => {
      await expect(admin.fetchOffsets({ groupId: 'groupId', topic: null })).rejects.toHaveProperty(
        'message',
        'Invalid topic null'
      )
    })

    test('returns unresolved consumer group offsets', async () => {
      const offsets = await admin.fetchOffsets({
        groupId,
        topic: topicName,
      })

      expect(offsets).toEqual([{ partition: 0, offset: '-1', metadata: null }])
    })

    test('returns the current consumer group offset', async () => {
      await admin.setOffsets({
        groupId,
        topic: topicName,
        partitions: [{ partition: 0, offset: 13 }],
      })

      const offsets = await admin.fetchOffsets({
        groupId,
        topic: topicName,
      })

      expect(offsets).toEqual([{ partition: 0, offset: '13', metadata: null }])
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
          topic: topicName,
          resolveOffsets: true,
        })
        const offsetsAfterResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
        })

        expect(offsetsBeforeResolving).toEqual([{ partition: 0, offset: '5', metadata: null }])
        expect(offsetsUponResolving).toEqual([{ partition: 0, offset: '5', metadata: null }])
        expect(offsetsAfterResolving).toEqual([{ partition: 0, offset: '5', metadata: null }])
      })

      test('reset to latest: returns latest *topic* offsets after resolving', async () => {
        await admin.resetOffsets({ groupId, topic: topicName })

        const offsetsBeforeResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
        })
        const offsetsUponResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
          resolveOffsets: true,
        })
        const offsetsAfterResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
        })

        expect(offsetsBeforeResolving).toEqual([{ partition: 0, offset: '-1', metadata: null }])
        expect(offsetsUponResolving).toEqual([{ partition: 0, offset: '10', metadata: null }])
        expect(offsetsAfterResolving).toEqual([{ partition: 0, offset: '10', metadata: null }])
      })

      test('reset to earliest: returns earliest *topic* offsets after resolving', async () => {
        await admin.resetOffsets({ groupId, topic: topicName, earliest: true })

        const offsetsBeforeResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
        })
        const offsetsUponResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
          resolveOffsets: true,
        })
        const offsetsAfterResolving = await admin.fetchOffsets({
          groupId,
          topic: topicName,
        })

        expect(offsetsBeforeResolving).toEqual([{ partition: 0, offset: '-2', metadata: null }])
        expect(offsetsUponResolving).toEqual([{ partition: 0, offset: '0', metadata: null }])
        expect(offsetsAfterResolving).toEqual([{ partition: 0, offset: '0', metadata: null }])
      })

      testIfKafkaAtLeast_0_11(
        'will return the correct earliest offset when it is greater than 0',
        async () => {
          // simulate earliest offset = 7, by deleting first 7 messages from the topic
          const messagesToDelete = [
            {
              topic: topicName,
              partitions: [
                {
                  partition: 0,
                  offset: '7',
                },
              ],
            },
          ]

          await deleteRecords({ cluster, topicName, topicDetails: messagesToDelete })
          await admin.resetOffsets({ groupId, topic: topicName, earliest: true })

          const offsetsBeforeResolving = await admin.fetchOffsets({
            groupId,
            topic: topicName,
          })
          const offsetsUponResolving = await admin.fetchOffsets({
            groupId,
            topic: topicName,
            resolveOffsets: true,
          })
          const offsetsAfterResolving = await admin.fetchOffsets({
            groupId,
            topic: topicName,
          })

          expect(offsetsBeforeResolving).toEqual([{ partition: 0, offset: '-2', metadata: null }])
          expect(offsetsUponResolving).toEqual([{ partition: 0, offset: '7', metadata: null }])
          expect(offsetsAfterResolving).toEqual([{ partition: 0, offset: '7', metadata: null }])
        }
      )
    })
  })
})

/**
 * Manually delete records up to the selected offset
 * http://kafka.apache.org/protocol.html#The_Messages_DeleteRecords
 * Only available from Kafka 0.11 upwards
 */
const deleteRecords = async ({ cluster, topicName, topicDetails }) => {
  const partitionLeader = await cluster.findLeaderForPartitions(topicName, [0])
  const [nodeId] = Object.keys(partitionLeader)

  const connection = (await cluster.findBroker({ nodeId })).connection
  const requestTimeout = 5000
  const request = {
    apiKey: 21,
    apiVersion: 0,
    encode: async () => {
      return new Encoder()
        .writeArray(
          topicDetails.map(({ topic, partitions }) => {
            return new Encoder().writeString(topic).writeArray(
              partitions.map(({ partition, offset }) => {
                return new Encoder().writeInt32(partition).writeInt64(offset)
              })
            )
          })
        )
        .writeInt32(requestTimeout)
    },
    expectResponse: () => true,
  }

  const response = {
    decode: async rawData => {
      const decoder = new Decoder(rawData)
      return {
        throttleTime: decoder.readInt32(),
        topics: decoder
          .readArray(decoder => ({
            topic: decoder.readString(),
            partitions: decoder.readArray(decoder => ({
              partitionIndex: decoder.readInt32(),
              lowWatermark: decoder.readInt64(),
              errorCode: decoder.readInt16(),
            })),
          }))
          .sort((a, b) => a.topic.localeCompare(b.topic)),
      }
    },
    parse: data => {
      let responseCode
      if (
        data.topics.some(({ partitions }) =>
          partitions.some(({ errorCode }) => {
            responseCode = errorCode
            return errorCode !== 0
          })
        )
      )
        throw new Error(`Unable to delete Kafka record. Returned code ${responseCode}`)
    },
  }

  await connection.send({ request, response })
}
