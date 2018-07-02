const flatten = require('../utils/flatten')
const sleep = require('../utils/sleep')
const arrayDiff = require('../utils/arrayDiff')
const OffsetManager = require('./offsetManager')
const Batch = require('./batch')
const SeekOffsets = require('./seekOffsets')
const SubscriptionState = require('./subscriptionState')
const { KafkaJSError, KafkaJSNonRetriableError } = require('../errors')
const { HEARTBEAT } = require('./instrumentationEvents')
const { MemberAssignment } = require('./assignerProtocol')

const { keys } = Object

const STALE_METADATA_ERRORS = [
  'LEADER_NOT_AVAILABLE',
  'NOT_LEADER_FOR_PARTITION',
  'UNKNOWN_TOPIC_OR_PARTITION',
]

module.exports = class ConsumerGroup {
  constructor({
    cluster,
    groupId,
    topics,
    topicConfigurations,
    logger,
    instrumentationEmitter,
    assigners,
    sessionTimeout,
    maxBytesPerPartition,
    minBytes,
    maxBytes,
    maxWaitTimeInMs,
  }) {
    this.cluster = cluster
    this.groupId = groupId
    this.topics = topics
    this.topicConfigurations = topicConfigurations
    this.logger = logger.namespace('ConsumerGroup')
    this.instrumentationEmitter = instrumentationEmitter
    this.assigners = assigners
    this.sessionTimeout = sessionTimeout
    this.maxBytesPerPartition = maxBytesPerPartition
    this.minBytes = minBytes
    this.maxBytes = maxBytes
    this.maxWaitTime = maxWaitTimeInMs

    this.seekOffset = new SeekOffsets()
    this.coordinator = null
    this.generationId = null
    this.leaderId = null
    this.memberId = null
    this.members = null
    this.groupProtocol = null

    this.memberAssignment = null
    this.offsetManager = null
    this.subscriptionState = new SubscriptionState()

    this.lastRequest = Date.now()
  }

  isLeader() {
    return this.leaderId && this.memberId === this.leaderId
  }

  async join() {
    const { groupId, sessionTimeout } = this

    this.coordinator = await this.cluster.findGroupCoordinator({ groupId })

    const groupData = await this.coordinator.joinGroup({
      groupId,
      sessionTimeout,
      memberId: this.memberId || '',
      groupProtocols: this.assigners.map(assigner => assigner.protocol({ topics: this.topics })),
    })

    this.generationId = groupData.generationId
    this.leaderId = groupData.leaderId
    this.memberId = groupData.memberId
    this.members = groupData.members
    this.groupProtocol = groupData.groupProtocol
  }

  async leave() {
    const { groupId, memberId } = this
    if (memberId) {
      await this.coordinator.leaveGroup({ groupId, memberId })
      this.memberId = null
    }
  }

  async sync() {
    let assignment = []
    const { groupId, generationId, memberId, members, groupProtocol, topics, coordinator } = this

    if (this.isLeader()) {
      this.logger.debug('Chosen as group leader', { groupId, generationId, memberId, topics })
      const assigner = this.assigners.find(({ name }) => name === groupProtocol)

      if (!assigner) {
        throw new KafkaJSNonRetriableError(
          `Unsupported partition assigner "${groupProtocol}", the assigner wasn't found in the assigners list`
        )
      }

      assignment = await assigner.assign({ members, topics })
      this.logger.debug('Group assignment', {
        groupId,
        generationId,
        topics,
        groupProtocol,
        assignment,
      })
    }

    const { memberAssignment } = await this.coordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupAssignment: assignment,
    })

    const decodedAssigment = MemberAssignment.decode(memberAssignment).assignment
    this.logger.debug('Received assignment', {
      groupId,
      generationId,
      memberId,
      memberAssignment: decodedAssigment,
    })

    let currentMemberAssignment = decodedAssigment
    const assignedTopics = keys(currentMemberAssignment)
    const topicsNotSubscribed = arrayDiff(assignedTopics, this.topics)

    if (topicsNotSubscribed.length > 0) {
      this.logger.warn('Consumer group received unsubscribed topics', {
        groupId,
        generationId,
        memberId,
        assignedTopics,
        topicsSubscribed: this.topics,
        topicsNotSubscribed,
      })

      // Remove unsubscribed topics from the list
      const safeAssignment = arrayDiff(assignedTopics, topicsNotSubscribed)
      currentMemberAssignment = safeAssignment.reduce(
        (assignment, topic) => ({ ...assignment, [topic]: decodedAssigment[topic] }),
        {}
      )
    }

    this.memberAssignment = currentMemberAssignment
    this.topics = keys(this.memberAssignment)
    this.offsetManager = new OffsetManager({
      cluster: this.cluster,
      topicConfigurations: this.topicConfigurations,
      instrumentationEmitter: this.instrumentationEmitter,
      memberAssignment: this.memberAssignment,
      coordinator,
      groupId,
      generationId,
      memberId,
    })
  }

  resetOffset({ topic, partition }) {
    this.offsetManager.resetOffset({ topic, partition })
  }

  resolveOffset({ topic, partition, offset }) {
    this.offsetManager.resolveOffset({ topic, partition, offset })
  }

  /**
   * Update the consumer offset for the given topic/partition. This will be used
   * on the next fetch. If this API is invoked for the same topic/partition more
   * than once, the latest offset will be used on the next fetch.
   *
   * @param {string} topic
   * @param {number} partition
   * @param {string} offset
   */
  seek({ topic, partition, offset }) {
    this.seekOffset.set(topic, partition, offset)
  }

  pause(topics) {
    this.logger.info(`Pausing fetching from ${topics.length} topics`, {
      topics,
    })
    this.subscriptionState.pause(topics)
  }

  resume(topics) {
    this.logger.info(`Resuming fetching from ${topics.length} topics`, {
      topics,
    })
    this.subscriptionState.resume(topics)
  }

  paused() {
    return this.subscriptionState.paused()
  }

  async commitOffsets() {
    await this.offsetManager.commitOffsets()
  }

  async heartbeat({ interval }) {
    const { groupId, generationId, memberId } = this
    const now = Date.now()
    if (now > this.lastRequest + interval) {
      const payload = {
        groupId,
        memberId,
        groupGenerationId: generationId,
      }
      await this.coordinator.heartbeat(payload)

      this.instrumentationEmitter.emit(HEARTBEAT, payload)
      this.lastRequest = Date.now()
    }
  }

  async fetch() {
    try {
      const { topics, maxBytesPerPartition, maxWaitTime, minBytes, maxBytes } = this
      const requestsPerLeader = {}

      while (this.seekOffset.size > 0) {
        const seekEntry = this.seekOffset.pop()
        this.logger.debug('Seek offset', {
          groupId: this.groupId,
          memberId: this.memberId,
          seek: seekEntry,
        })
        await this.offsetManager.seek(seekEntry)
      }

      await this.offsetManager.resolveOffsets()

      const pausedTopics = this.subscriptionState.paused()
      const activeTopics = topics.filter(topic => !pausedTopics.includes(topic))

      if (activeTopics.length === 0) {
        this.logger.debug(`No active topics, sleeping for ${this.maxWaitTime}ms`, {
          topics,
          activeTopics,
          pausedTopics,
        })

        await sleep(this.maxWaitTime)
        return []
      }

      this.logger.debug(`Fetching from ${activeTopics.length} out of ${topics.length} topics`, {
        topics,
        activeTopics,
        pausedTopics,
      })

      for (let topic of activeTopics) {
        const partitionsPerLeader = this.cluster.findLeaderForPartitions(
          topic,
          this.memberAssignment[topic]
        )

        const leaders = keys(partitionsPerLeader)

        for (let leader of leaders) {
          const partitions = partitionsPerLeader[leader].map(partition => ({
            partition,
            fetchOffset: this.offsetManager.nextOffset(topic, partition),
            maxBytes: maxBytesPerPartition,
          }))

          requestsPerLeader[leader] = requestsPerLeader[leader] || []
          requestsPerLeader[leader].push({ topic, partitions })
        }
      }

      const requests = keys(requestsPerLeader).map(async nodeId => {
        const broker = await this.cluster.findBroker({ nodeId })
        const { responses } = await broker.fetch({
          maxWaitTime,
          minBytes,
          maxBytes,
          topics: requestsPerLeader[nodeId],
        })

        const batchesPerPartition = responses.map(({ topicName, partitions }) => {
          return partitions.map(partition => new Batch(topicName, partition))
        })

        return flatten(batchesPerPartition)
      })

      const results = await Promise.all(requests)
      return flatten(results)
    } catch (e) {
      if (STALE_METADATA_ERRORS.includes(e.type) || e.name === 'KafkaJSTopicMetadataNotLoaded') {
        this.logger.debug('Stale cluster metadata, refreshing...', {
          groupId: this.groupId,
          memberId: this.memberId,
          error: e.message,
        })

        await this.cluster.refreshMetadata()
        await this.join()
        await this.sync()
        throw new KafkaJSError(e.message)
      }

      if (e.name === 'KafkaJSOffsetOutOfRange') {
        await this.recoverFromOffsetOutOfRange(e)
      }

      if (e.name === 'KafkaJSBrokerNotFound') {
        this.logger.debug(`${e.message}, refreshing metadata and retrying...`)
        await this.cluster.refreshMetadata()
      }

      throw e
    }
  }

  async recoverFromOffsetOutOfRange(e) {
    this.logger.error('Offset out of range, resetting to default offset', {
      topic: e.topic,
      partition: e.partition,
      groupId: this.groupId,
      memberId: this.memberId,
    })

    await this.offsetManager.setDefaultOffset({
      topic: e.topic,
      partition: e.partition,
    })
  }
}
