const flatten = require('../utils/flatten')
const sleep = require('../utils/sleep')
const websiteUrl = require('../utils/websiteUrl')
const arrayDiff = require('../utils/arrayDiff')
const OffsetManager = require('./offsetManager')
const Batch = require('./batch')
const SeekOffsets = require('./seekOffsets')
const SubscriptionState = require('./subscriptionState')
const {
  events: { HEARTBEAT },
} = require('./instrumentationEvents')
const { MemberAssignment } = require('./assignerProtocol')
const {
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSStaleTopicMetadataAssignment,
} = require('../errors')

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
    rebalanceTimeout,
    maxBytesPerPartition,
    minBytes,
    maxBytes,
    maxWaitTimeInMs,
    autoCommitInterval,
    autoCommitThreshold,
    isolationLevel,
  }) {
    this.cluster = cluster
    this.groupId = groupId
    this.topics = topics
    this.topicsSubscribed = topics
    this.topicConfigurations = topicConfigurations
    this.logger = logger.namespace('ConsumerGroup')
    this.instrumentationEmitter = instrumentationEmitter
    this.assigners = assigners
    this.sessionTimeout = sessionTimeout
    this.rebalanceTimeout = rebalanceTimeout
    this.maxBytesPerPartition = maxBytesPerPartition
    this.minBytes = minBytes
    this.maxBytes = maxBytes
    this.maxWaitTime = maxWaitTimeInMs
    this.autoCommitInterval = autoCommitInterval
    this.autoCommitThreshold = autoCommitThreshold
    this.isolationLevel = isolationLevel

    this.seekOffset = new SeekOffsets()
    this.coordinator = null
    this.generationId = null
    this.leaderId = null
    this.memberId = null
    this.members = null
    this.groupProtocol = null

    this.partitionsPerSubscribedTopic = null
    this.offsetManager = null
    this.subscriptionState = new SubscriptionState()

    this.lastRequest = Date.now()
  }

  isLeader() {
    return this.leaderId && this.memberId === this.leaderId
  }

  async connect() {
    await this.cluster.connect()
    await this.cluster.refreshMetadataIfNecessary()
  }

  async join() {
    const { groupId, sessionTimeout, rebalanceTimeout } = this

    this.coordinator = await this.cluster.findGroupCoordinator({ groupId })

    const groupData = await this.coordinator.joinGroup({
      groupId,
      sessionTimeout,
      rebalanceTimeout,
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
    const {
      groupId,
      generationId,
      memberId,
      members,
      groupProtocol,
      topics,
      topicsSubscribed,
      coordinator,
    } = this

    if (this.isLeader()) {
      this.logger.debug('Chosen as group leader', { groupId, generationId, memberId, topics })
      const assigner = this.assigners.find(({ name }) => name === groupProtocol)

      if (!assigner) {
        throw new KafkaJSNonRetriableError(
          `Unsupported partition assigner "${groupProtocol}", the assigner wasn't found in the assigners list`
        )
      }

      await this.cluster.refreshMetadata()
      assignment = await assigner.assign({ members, topics: topicsSubscribed })

      this.logger.debug('Group assignment', {
        groupId,
        generationId,
        groupProtocol,
        assignment,
        topics: topicsSubscribed,
      })
    }

    // Keep track of the partitions for the subscribed topics
    this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()
    const { memberAssignment } = await this.coordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupAssignment: assignment,
    })

    const decodedAssignment = MemberAssignment.decode(memberAssignment).assignment
    this.logger.debug('Received assignment', {
      groupId,
      generationId,
      memberId,
      memberAssignment: decodedAssignment,
    })

    const assignedTopics = keys(decodedAssignment)
    const topicsNotSubscribed = arrayDiff(assignedTopics, topicsSubscribed)

    if (topicsNotSubscribed.length > 0) {
      this.logger.warn('Consumer group received unsubscribed topics', {
        groupId,
        generationId,
        memberId,
        assignedTopics,
        topicsSubscribed,
        topicsNotSubscribed,
        helpUrl: websiteUrl(
          'docs/faq',
          'why-am-i-receiving-messages-for-topics-i-m-not-subscribed-to'
        ),
      })
    }

    // Remove unsubscribed topics from the list
    const safeAssignment = arrayDiff(assignedTopics, topicsNotSubscribed)
    const currentMemberAssignment = safeAssignment.map(topic => ({
      topic,
      partitions: decodedAssignment[topic],
    }))

    // Check if the consumer is aware of all assigned partitions
    for (const assignment of currentMemberAssignment) {
      const { topic, partitions: assignedPartitions } = assignment
      const knownPartitions = this.partitionsPerSubscribedTopic.get(topic)
      const isAwareOfAllAssignedPartitions = assignedPartitions.every(partition =>
        knownPartitions.includes(partition)
      )

      if (!isAwareOfAllAssignedPartitions) {
        this.logger.warn('Consumer is not aware of all assigned partitions, refreshing metadata', {
          groupId,
          generationId,
          memberId,
          topic,
          knownPartitions,
          assignedPartitions,
        })

        // If the consumer is not aware of all assigned partions, refresh metadata
        // and update the list of partitions per subscribed topic. It's enough to perform
        // this operation once since refresh metadata will update metadata for all topics
        await this.cluster.refreshMetadata()
        this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()
        break
      }
    }

    this.topics = currentMemberAssignment.map(({ topic }) => topic)
    this.subscriptionState.assign(currentMemberAssignment)
    this.offsetManager = new OffsetManager({
      cluster: this.cluster,
      topicConfigurations: this.topicConfigurations,
      instrumentationEmitter: this.instrumentationEmitter,
      memberAssignment: currentMemberAssignment.reduce(
        (partitionsByTopic, { topic, partitions }) => ({
          ...partitionsByTopic,
          [topic]: partitions,
        }),
        {}
      ),
      autoCommitInterval: this.autoCommitInterval,
      autoCommitThreshold: this.autoCommitThreshold,
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

  pause(topicPartitions) {
    this.logger.info(`Pausing fetching from ${topicPartitions.length} topics`, {
      topicPartitions,
    })
    this.subscriptionState.pause(topicPartitions)
  }

  resume(topicPartitions) {
    this.logger.info(`Resuming fetching from ${topicPartitions.length} topics`, {
      topicPartitions,
    })
    this.subscriptionState.resume(topicPartitions)
  }

  assigned() {
    return this.subscriptionState.assigned()
  }

  paused() {
    return this.subscriptionState.paused()
  }

  async commitOffsetsIfNecessary() {
    await this.offsetManager.commitOffsetsIfNecessary()
  }

  async commitOffsets(offsets) {
    await this.offsetManager.commitOffsets(offsets)
  }

  uncommittedOffsets() {
    return this.offsetManager.uncommittedOffsets()
  }

  async heartbeat({ interval }) {
    const { groupId, generationId, memberId } = this
    const now = Date.now()

    if (memberId && now >= this.lastRequest + interval) {
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

      await this.cluster.refreshMetadataIfNecessary()
      this.checkForStaleAssignment()

      while (this.seekOffset.size > 0) {
        const seekEntry = this.seekOffset.pop()
        this.logger.debug('Seek offset', {
          groupId: this.groupId,
          memberId: this.memberId,
          seek: seekEntry,
        })
        await this.offsetManager.seek(seekEntry)
      }

      const pausedTopicPartitions = this.subscriptionState.paused()
      const activeTopicPartitions = this.subscriptionState.active()

      const activePartitions = flatten(activeTopicPartitions.map(({ partitions }) => partitions))
      const activeTopics = activeTopicPartitions
        .filter(({ partitions }) => partitions.length > 0)
        .map(({ topic }) => topic)

      if (activePartitions.length === 0) {
        this.logger.debug(`No active topic partitions, sleeping for ${this.maxWaitTime}ms`, {
          topics,
          activeTopicPartitions,
          pausedTopicPartitions,
        })

        await sleep(this.maxWaitTime)
        return []
      }

      await this.offsetManager.resolveOffsets()

      this.logger.debug(
        `Fetching from ${activePartitions.length} partitions for ${activeTopics.length} out of ${topics.length} topics`,
        {
          topics,
          activeTopicPartitions,
          pausedTopicPartitions,
        }
      )

      for (const topicPartition of activeTopicPartitions) {
        const partitionsPerLeader = this.cluster.findLeaderForPartitions(
          topicPartition.topic,
          topicPartition.partitions
        )

        const leaders = keys(partitionsPerLeader)

        for (const leader of leaders) {
          const partitions = partitionsPerLeader[leader].map(partition => ({
            partition,
            fetchOffset: this.offsetManager.nextOffset(topicPartition.topic, partition).toString(),
            maxBytes: maxBytesPerPartition,
          }))

          requestsPerLeader[leader] = requestsPerLeader[leader] || []
          requestsPerLeader[leader].push({ topic: topicPartition.topic, partitions })
        }
      }

      const requests = keys(requestsPerLeader).map(async nodeId => {
        const broker = await this.cluster.findBroker({ nodeId })
        const { responses } = await broker.fetch({
          maxWaitTime,
          minBytes,
          maxBytes,
          isolationLevel: this.isolationLevel,
          topics: requestsPerLeader[nodeId],
        })

        const batchesPerPartition = responses.map(({ topicName, partitions }) => {
          const topicRequestData = requestsPerLeader[nodeId].find(
            ({ topic }) => topic === topicName
          )

          return partitions
            .filter(
              partitionData =>
                !this.seekOffset.has(topicName, partitionData.partition) &&
                !this.subscriptionState.isPaused(topicName, partitionData.partition)
            )
            .map(partitionData => {
              const partitionRequestData = topicRequestData.partitions.find(
                ({ partition }) => partition === partitionData.partition
              )

              const fetchedOffset = partitionRequestData.fetchOffset

              return new Batch(topicName, fetchedOffset, partitionData)
            })
        })

        return flatten(batchesPerPartition)
      })

      // fetch can generate empty requests when the consumer group receives an assignment
      // with more topics than the subscribed, so to prevent a busy loop we wait the
      // configured max wait time
      if (requests.length === 0) {
        await sleep(this.maxWaitTime)
        return []
      }

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

      if (e.name === 'KafkaJSStaleTopicMetadataAssignment') {
        this.logger.warn(`${e.message}, resync group`, {
          groupId: this.groupId,
          memberId: this.memberId,
          topic: e.topic,
          unknownPartitions: e.unknownPartitions,
        })

        await this.join()
        await this.sync()
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

  generatePartitionsPerSubscribedTopic() {
    const map = new Map()

    for (const topic of this.topicsSubscribed) {
      const partitions = this.cluster
        .findTopicPartitionMetadata(topic)
        .map(m => m.partitionId)
        .sort()

      map.set(topic, partitions)
    }

    return map
  }

  checkForStaleAssignment() {
    if (!this.partitionsPerSubscribedTopic) {
      return
    }

    const newPartitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()

    for (const [topic, partitions] of newPartitionsPerSubscribedTopic) {
      const diff = arrayDiff(partitions, this.partitionsPerSubscribedTopic.get(topic))

      if (diff.length > 0) {
        throw new KafkaJSStaleTopicMetadataAssignment('Topic has been updated', {
          topic,
          unknownPartitions: diff,
        })
      }
    }
  }

  hasSeekOffset({ topic, partition }) {
    return this.seekOffset.has(topic, partition)
  }
}
