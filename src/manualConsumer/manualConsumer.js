const sleep = require('../utils/sleep')
const arrayDiff = require('../utils/arrayDiff')
const createRetry = require('../retry')

const OffsetManager = require('./offsetManager')
const Batch = require('../consumer/batch')
const SeekOffsets = require('../consumer/seekOffsets')
const SubscriptionState = require('../consumer/subscriptionState')
const {
  events: { CONNECT },
} = require('../consumer/instrumentationEvents')
const { KafkaJSError, KafkaJSStaleTopicMetadataAssignment } = require('../errors')

const STALE_METADATA_ERRORS = [
  'LEADER_NOT_AVAILABLE',
  // Fetch before v9 uses NOT_LEADER_FOR_PARTITION
  'NOT_LEADER_FOR_PARTITION',
  // Fetch after v9 uses {FENCED,UNKNOWN}_LEADER_EPOCH
  'FENCED_LEADER_EPOCH',
  'UNKNOWN_LEADER_EPOCH',
  'UNKNOWN_TOPIC_OR_PARTITION',
]

module.exports = class ManualConsumer {
  /**
   * @param {object} options
   * @param {import('../../types').RetryOptions} options.retry
   * @param {import('../../types').Cluster} options.cluster
   * @param {string[]} options.topics
   * @param {Record<string, { fromBeginning?: boolean }>} options.topicConfigurations
   * @param {import('../../types').Logger} options.logger
   * @param {import('../instrumentation/emitter')} options.instrumentationEmitter
   * @param {number} options.maxBytesPerPartition
   * @param {number} options.minBytes
   * @param {number} options.maxBytes
   * @param {number} options.maxWaitTimeInMs
   * @param {number} options.isolationLevel
   * @param {string} options.rackId
   * @param {number} options.metadataMaxAge
   */
  constructor({
    retry,
    cluster,
    topics,
    topicConfigurations,
    logger,
    instrumentationEmitter,
    maxBytesPerPartition,
    minBytes,
    maxBytes,
    maxWaitTimeInMs,
    isolationLevel,
    rackId,
    metadataMaxAge,
  }) {
    /** @type {import("../../types").Cluster} */
    this.cluster = cluster
    this.topics = topics
    this.topicsSubscribed = topics
    this.topicConfigurations = topicConfigurations
    this.logger = logger.namespace('ManualConsumer')
    this.instrumentationEmitter = instrumentationEmitter
    this.retrier = createRetry(Object.assign({}, retry))
    this.maxBytesPerPartition = maxBytesPerPartition
    this.minBytes = minBytes
    this.maxBytes = maxBytes
    this.maxWaitTime = maxWaitTimeInMs
    this.isolationLevel = isolationLevel
    this.rackId = rackId
    this.metadataMaxAge = metadataMaxAge

    this.seekOffset = new SeekOffsets()

    this.partitionsPerSubscribedTopic = null
    /**
     * Preferred read replica per topic and partition
     *
     * Each of the partitions tracks the preferred read replica (`nodeId`) and a timestamp
     * until when that preference is valid.
     *
     * @type {{[topicName: string]: {[partition: number]: {nodeId: number, expireAt: number}}}}
     */
    this.preferredReadReplicasPerTopicPartition = {}
    this.offsetManager = null
    this.subscriptionState = new SubscriptionState()
  }

  getNodeIds() {
    return this.cluster.getNodeIds()
  }

  async connect() {
    await this.cluster.connect()
    this.instrumentationEmitter.emit(CONNECT)
    await this.cluster.refreshMetadataIfNecessary()
  }

  async assignPartitions() {
    const memberAssignment = this.topics.map(topic => {
      const partitionMetadata = this.cluster.findTopicPartitionMetadata(topic)
      return {
        topic,
        partitions: partitionMetadata.map(metadata => metadata.partitionId),
      }
    })
    this.subscriptionState.assign(memberAssignment)
    this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()

    const offsetManagerMemberAssignment = memberAssignment.reduce(
      (partitionsByTopic, { topic, partitions }) => ({
        ...partitionsByTopic,
        [topic]: partitions,
      }),
      {}
    )

    if (!this.offsetManager) {
      this.offsetManager = new OffsetManager({
        cluster: this.cluster,
        topicConfigurations: this.topicConfigurations,
        instrumentationEmitter: this.instrumentationEmitter,
        memberAssignment: offsetManagerMemberAssignment,
      })
    } else {
      // we're re-assigning partitions due to metadata updates:
      // preserve the resolved offsets for existing partitions by only updating
      // the member assignment rather than re-instantiating the offset manager.
      this.offsetManager.updateMemberAssignment(offsetManagerMemberAssignment)
    }
    // Note that fetch can be running concurrently with offset resolution
    await this.offsetManager.resolveOffsets()
  }

  /**
   * @param {import("../../types").TopicPartitionOffset} topicPartitionOffset
   */
  resolveOffset({ topic, partition, offset }) {
    this.offsetManager.resolveOffset({ topic, partition, offset })
  }

  /**
   * Update the offset for the given topic/partition. This will be used
   * on the next fetch. If this API is invoked for the same topic/partition more
   * than once, the latest offset will be used on the next fetch.
   *
   * @param {import("../../types").TopicPartitionOffset} topicPartitionOffset
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

  /**
   * @param {string} topic
   * @param {string} partition
   * @returns {boolean} whether the specified topic-partition are paused or not
   */
  isPaused(topic, partition) {
    return this.subscriptionState.isPaused(topic, partition)
  }

  async fetch(nodeId) {
    try {
      await this.cluster.refreshMetadataIfNecessary()
      this.checkForStaleAssignment()

      let topicPartitions = this.subscriptionState.assigned()
      topicPartitions = this.filterPartitionsByNode(nodeId, topicPartitions)

      await this.seekOffsets(topicPartitions)

      const activeTopicPartitions = this.getActiveTopicPartitions()

      const requests = topicPartitions
        .map(({ topic, partitions }) => ({
          topic,
          partitions: partitions
            .filter(
              partition =>
                /**
                 * After partitions are added, the consumer can be resolving offsets
                 * for the new partitions when new fetch requests are started.
                 * Any offsets set now would then be overwritten by resolveOffsets
                 * once it has completed, so we skip partitions that have not had their
                 * offsets resolved yet.
                 */
                this.offsetManager.resolvedOffsets[topic][partition] !== undefined &&
                activeTopicPartitions[topic].has(partition)
            )
            .map(partition => ({
              partition,
              fetchOffset: this.offsetManager.nextOffset(topic, partition).toString(),
              maxBytes: this.maxBytesPerPartition,
            })),
        }))
        .filter(({ partitions }) => partitions.length)

      if (!requests.length) {
        await sleep(this.maxWaitTime)
        return []
      }

      const broker = await this.cluster.findBroker({ nodeId })

      const { responses } = await broker.fetch({
        maxWaitTime: this.maxWaitTime,
        minBytes: this.minBytes,
        maxBytes: this.maxBytes,
        isolationLevel: this.isolationLevel,
        topics: requests,
        rackId: this.rackId,
      })

      return responses.flatMap(({ topicName, partitions }) => {
        const topicRequestData = requests.find(({ topic }) => topic === topicName)

        let preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[topicName]
        if (!preferredReadReplicas) {
          this.preferredReadReplicasPerTopicPartition[topicName] = preferredReadReplicas = {}
        }

        return partitions
          .filter(
            ({ partition }) =>
              !this.seekOffset.has(topicName, partition) &&
              !this.subscriptionState.isPaused(topicName, partition)
          )
          .map(partitionData => {
            const { partition, preferredReadReplica } = partitionData

            if (preferredReadReplica != null && preferredReadReplica !== -1) {
              const { nodeId: currentPreferredReadReplica } = preferredReadReplicas[partition] || {}
              if (currentPreferredReadReplica !== preferredReadReplica) {
                this.logger.info(`Preferred read replica is now ${preferredReadReplica}`, {
                  topic: topicName,
                  partition,
                })
              }
              preferredReadReplicas[partition] = {
                nodeId: preferredReadReplica,
                expireAt: Date.now() + this.metadataMaxAge,
              }
            }

            const partitionRequestData = topicRequestData.partitions.find(
              ({ partition }) => partition === partitionData.partition
            )

            const fetchedOffset = partitionRequestData.fetchOffset
            return new Batch(topicName, fetchedOffset, partitionData)
          })
      })
    } catch (e) {
      await this.recoverFromFetch(e)
      return []
    }
  }

  async recoverFromFetch(e) {
    if (STALE_METADATA_ERRORS.includes(e.type) || e.name === 'KafkaJSTopicMetadataNotLoaded') {
      this.logger.debug('Stale cluster metadata, refreshing...', {
        error: e.message,
      })

      await this.cluster.refreshMetadata()
      await this.assignPartitions()
      return
    }

    if (e.name === 'KafkaJSStaleTopicMetadataAssignment') {
      this.logger.warn(`${e.message}, resync`, {
        topic: e.topic,
        unknownPartitions: e.unknownPartitions,
      })
      await this.assignPartitions()
      return
    }

    if (e.name === 'KafkaJSOffsetOutOfRange') {
      await this.recoverFromOffsetOutOfRange(e)
      return
    }

    if (e.name === 'KafkaJSConnectionClosedError') {
      this.cluster.removeBroker({ host: e.host, port: e.port })
      return
    }

    if (e.name === 'KafkaJSBrokerNotFound' || e.name === 'KafkaJSConnectionClosedError') {
      this.logger.debug(`${e.message}, refreshing metadata and retrying...`)
      await this.cluster.refreshMetadata()
      return
    }

    throw e
  }

  async recoverFromOffsetOutOfRange(e) {
    // If we are fetching from a follower try with the leader before resetting offsets
    const preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[e.topic]
    if (preferredReadReplicas && typeof preferredReadReplicas[e.partition] === 'number') {
      this.logger.info('Offset out of range while fetching from follower, retrying with leader', {
        topic: e.topic,
        partition: e.partition,
      })
      delete preferredReadReplicas[e.partition]
    } else {
      this.logger.error('Offset out of range, resetting to default offset', {
        topic: e.topic,
        partition: e.partition,
      })

      this.offsetManager.setDefaultOffset({
        topic: e.topic,
        partition: e.partition,
      })
    }
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

  async seekOffsets(topicPartitions) {
    for (const { topic, partitions } of topicPartitions) {
      for (const partition of partitions) {
        const seekEntry = this.seekOffset.pop(topic, partition)
        if (!seekEntry) {
          continue
        }

        this.logger.debug('Seek offset', {
          seek: seekEntry,
        })
        this.offsetManager.seek(seekEntry)
      }
    }
  }

  hasSeekOffset({ topic, partition }) {
    return this.seekOffset.has(topic, partition)
  }

  /**
   * For each of the partitions find the best nodeId to read it from
   *
   * @param {string} topic
   * @param {number[]} partitions
   * @returns {{[nodeId: number]: number[]}} per-node assignment of partitions
   * @see Cluster~findLeaderForPartitions
   */
  // Invariant: The resulting object has each partition referenced exactly once
  findReadReplicaForPartitions(topic, partitions) {
    const partitionMetadata = this.cluster.findTopicPartitionMetadata(topic)
    const preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[topic]
    return partitions.reduce((result, id) => {
      const partitionId = parseInt(id, 10)
      const metadata = partitionMetadata.find(p => p.partitionId === partitionId)
      if (!metadata) {
        return result
      }

      if (metadata.leader == null) {
        throw new KafkaJSError('Invalid partition metadata', { topic, partitionId, metadata })
      }

      // Pick the preferred replica if there is one, and it isn't known to be offline, otherwise the leader.
      let nodeId = metadata.leader
      if (preferredReadReplicas) {
        const { nodeId: preferredReadReplica, expireAt } = preferredReadReplicas[partitionId] || {}
        if (Date.now() >= expireAt) {
          this.logger.debug('Preferred read replica information has expired, using leader', {
            topic,
            partitionId,
            preferredReadReplica,
            leader: metadata.leader,
          })
          // Drop the entry
          delete preferredReadReplicas[partitionId]
        } else if (preferredReadReplica != null) {
          // Valid entry, check whether it is not offline
          // Note that we don't delete the preference here, and rather hope that eventually that replica comes online again
          const offlineReplicas = metadata.offlineReplicas
          if (Array.isArray(offlineReplicas) && offlineReplicas.includes(nodeId)) {
            this.logger.debug('Preferred read replica is offline, using leader', {
              topic,
              partitionId,
              preferredReadReplica,
              leader: metadata.leader,
            })
          } else {
            nodeId = preferredReadReplica
          }
        }
      }
      const current = result[nodeId] || []
      return { ...result, [nodeId]: [...current, partitionId] }
    }, {})
  }

  filterPartitionsByNode(nodeId, topicPartitions) {
    return topicPartitions.map(({ topic, partitions }) => ({
      topic,
      partitions: this.findReadReplicaForPartitions(topic, partitions)[nodeId] || [],
    }))
  }

  getActiveTopicPartitions() {
    return this.subscriptionState
      .active()
      .reduce((acc, { topic, partitions }) => ({ ...acc, [topic]: new Set(partitions) }), {})
  }
}
