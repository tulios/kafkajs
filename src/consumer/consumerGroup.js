const flatten = require('../utils/flatten')
const OffsetManager = require('./offsetManager')
const Batch = require('./batch')
const { KafkaJSError } = require('../errors')
const { HEARTBEAT } = require('./instrumentationEvents')

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
    assigner,
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
    this.assigner = assigner
    this.sessionTimeout = sessionTimeout
    this.maxBytesPerPartition = maxBytesPerPartition
    this.minBytes = minBytes
    this.maxBytes = maxBytes
    this.maxWaitTime = maxWaitTimeInMs

    this.coordinator = null
    this.generationId = null
    this.leaderId = null
    this.memberId = null
    this.members = null

    this.memberAssignment = null
    this.offsetManager = null

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
    })

    this.generationId = groupData.generationId
    this.leaderId = groupData.leaderId
    this.memberId = groupData.memberId
    this.members = groupData.members
  }

  async leave() {
    const { groupId, memberId } = this
    if (memberId) {
      await this.coordinator.leaveGroup({ groupId, memberId })
    }
  }

  async sync() {
    let assignment = []
    const { groupId, generationId, memberId, members, topics, coordinator } = this

    if (this.isLeader()) {
      this.logger.debug('Chosen as group leader', { groupId, generationId, memberId, topics })
      assignment = this.assigner({ members, topics })
      this.logger.debug('Group assignment', { groupId, generationId, topics, assignment })
    }

    const { memberAssignment } = await this.coordinator.syncGroup({
      groupId,
      generationId,
      memberId,
      groupAssignment: assignment,
    })

    this.memberAssignment = memberAssignment
    this.topics = keys(this.memberAssignment)
    this.offsetManager = new OffsetManager({
      cluster: this.cluster,
      topicConfigurations: this.topicConfigurations,
      instrumentationEmitter: this.instrumentationEmitter,
      coordinator,
      memberAssignment,
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
      await this.offsetManager.resolveOffsets()

      for (let topic of topics) {
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
      if (STALE_METADATA_ERRORS.includes(e.type)) {
        this.logger.debug('Stale cluster metadata, refreshing...', {
          groupId: this.groupId,
          memberId: this.memberId,
        })

        await this.cluster.refreshMetadata()
        await this.join()
        await this.sync()
        throw new KafkaJSError(e.message)
      }

      if (e.name === 'KafkaJSOffsetOutOfRange') {
        this.logger.error('Offset out of range, resetting to default offset', {
          topic: e.topic,
          groupId: this.groupId,
          memberId: this.memberId,
          partition: e.partition,
        })

        await this.offsetManager.setDefaultOffset({
          topic: e.topic,
          partition: e.partition,
        })
      }

      if (e.name === 'KafkaJSBrokerNotFound') {
        await this.cluster.refreshMetadata()
      }

      throw e
    }
  }
}
