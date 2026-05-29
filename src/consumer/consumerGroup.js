const sleep = require('../utils/sleep')
const websiteUrl = require('../utils/websiteUrl')
const arrayDiff = require('../utils/arrayDiff')
const createRetry = require('../retry')
const sharedPromiseTo = require('../utils/sharedPromiseTo')

const OffsetManager = require('./offsetManager')
const Batch = require('./batch')
const SeekOffsets = require('./seekOffsets')
const SubscriptionState = require('./subscriptionState')
const {
  events: { GROUP_JOIN, HEARTBEAT, CONNECT, RECEIVED_UNSUBSCRIBED_TOPICS },
} = require('./instrumentationEvents')
const { MemberAssignment } = require('./assignerProtocol')
const {
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSStaleTopicMetadataAssignment,
  isRebalancing,
} = require('../errors')

const { keys } = Object

const STALE_METADATA_ERRORS = [
  'LEADER_NOT_AVAILABLE',
  // Fetch before v9 uses NOT_LEADER_FOR_PARTITION
  'NOT_LEADER_FOR_PARTITION',
  // Fetch after v9 uses {FENCED,UNKNOWN}_LEADER_EPOCH
  'FENCED_LEADER_EPOCH',
  'UNKNOWN_LEADER_EPOCH',
  'UNKNOWN_TOPIC_OR_PARTITION',
]

// Cooperative rebalancing normally converges in 2 rounds (revoke → rejoin →
// assign). Beyond this, the degraded path commits + warns and proceeds rather
// than looping forever (red-team S5).
const MAX_COOPERATIVE_ROUNDS = 2

const PRIVATE = {
  JOIN: Symbol('private:ConsumerGroup:join'),
  SYNC: Symbol('private:ConsumerGroup:sync'),
  SHARED_HEARTBEAT: Symbol('private:ConsumerGroup:sharedHeartbeat'),
  SHARED_JOIN_AND_SYNC: Symbol('private:ConsumerGroup:sharedJoinAndSync'),
}

module.exports = class ConsumerGroup {
  /**
   * @param {object} options
   * @param {import('../../types').RetryOptions} options.retry
   * @param {import('../../types').Cluster} options.cluster
   * @param {string} options.groupId
   * @param {string[]} options.topics
   * @param {Record<string, { fromBeginning?: boolean }>} options.topicConfigurations
   * @param {import('../../types').Logger} options.logger
   * @param {import('../instrumentation/emitter')} options.instrumentationEmitter
   * @param {import('../../types').Assigner[]} options.assigners
   * @param {number} options.sessionTimeout
   * @param {number} options.rebalanceTimeout
   * @param {number} options.maxBytesPerPartition
   * @param {number} options.minBytes
   * @param {number} options.maxBytes
   * @param {number} options.maxWaitTimeInMs
   * @param {boolean} options.autoCommit
   * @param {number} options.autoCommitInterval
   * @param {number} options.autoCommitThreshold
   * @param {number} options.isolationLevel
   * @param {string} options.rackId
   * @param {number} options.metadataMaxAge
   */
  constructor({
    retry,
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
    autoCommit,
    autoCommitInterval,
    autoCommitThreshold,
    isolationLevel,
    rackId,
    metadataMaxAge,
  }) {
    /** @type {import("../../types").Cluster} */
    this.cluster = cluster
    this.groupId = groupId
    this.topics = topics
    this.topicsSubscribed = topics
    this.topicConfigurations = topicConfigurations
    this.logger = logger.namespace('ConsumerGroup')
    this.instrumentationEmitter = instrumentationEmitter
    this.retrier = createRetry(Object.assign({}, retry))
    this.assigners = assigners
    this.sessionTimeout = sessionTimeout
    this.rebalanceTimeout = rebalanceTimeout
    this.maxBytesPerPartition = maxBytesPerPartition
    this.minBytes = minBytes
    this.maxBytes = maxBytes
    this.maxWaitTime = maxWaitTimeInMs
    this.autoCommit = autoCommit
    this.autoCommitInterval = autoCommitInterval
    this.autoCommitThreshold = autoCommitThreshold
    this.isolationLevel = isolationLevel
    this.rackId = rackId
    this.metadataMaxAge = metadataMaxAge

    this.seekOffset = new SeekOffsets()
    this.coordinator = null
    this.generationId = null
    this.leaderId = null
    this.memberId = null
    this.members = null
    this.groupProtocol = null

    this.partitionsPerSubscribedTopic = null

    // Cooperative (incremental) rebalancing state. Eager assigners never touch
    // these. `ownedGenerationId` tracks the generation the currently-owned
    // partitions were assigned under (S3); `rejoinNeeded` signals the runner to
    // run a second JOIN→SYNC round after a cooperative revoke (S1/S2).
    this.rejoinNeeded = false
    this.ownedGenerationId = -1
    this.cooperativeRounds = 0
    this.lastRevoked = []
    this.lastAdded = []

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

    this.lastRequest = Date.now()

    this[PRIVATE.SHARED_HEARTBEAT] = sharedPromiseTo(async ({ interval }) => {
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
    })

    // Serialize join+sync. Multiple fetchers can each hit stale metadata and
    // call joinAndSync() concurrently via recoverFromFetch(); in the
    // cooperative branch that would interleave reads/writes of subscriptionState
    // and the in-place OffsetManager and compute wrong revoked/added deltas.
    // sharedPromiseTo collapses concurrent calls into the in-flight one.
    this[PRIVATE.SHARED_JOIN_AND_SYNC] = sharedPromiseTo(() => this.joinAndSyncOnce())
  }

  isLeader() {
    return this.leaderId && this.memberId === this.leaderId
  }

  /**
   * The assigner selected by the group's negotiated protocol, or null before
   * the first JOIN. Cooperative behavior is gated on this assigner's
   * `rebalanceProtocol` flag, never a name string (red-team N1).
   *
   * @returns {import('../../types').Assigner | null}
   */
  selectedAssigner() {
    if (!this.groupProtocol) return null
    return this.assigners.find(({ name }) => name === this.groupProtocol) || null
  }

  /**
   * @returns {boolean} whether the negotiated assigner uses cooperative rebalancing
   */
  isCooperative() {
    const assigner = this.selectedAssigner()
    return Boolean(assigner && assigner.rebalanceProtocol === 'cooperative')
  }

  /**
   * Currently-owned partitions in the `{ [topic]: number[] }` shape the
   * cooperative assigner advertises in JOIN.
   *
   * @returns {Object<string, number[]>}
   */
  ownedPartitions() {
    return this.subscriptionState.assigned().reduce((owned, { topic, partitions }) => {
      owned[topic] = partitions
      return owned
    }, {})
  }

  getNodeIds() {
    return this.cluster.getNodeIds()
  }

  async connect() {
    await this.cluster.connect()
    this.instrumentationEmitter.emit(CONNECT)
    await this.cluster.refreshMetadataIfNecessary()
  }

  async [PRIVATE.JOIN]() {
    const { groupId, sessionTimeout, rebalanceTimeout } = this

    this.coordinator = await this.cluster.findGroupCoordinator({ groupId })

    // Cooperative members advertise the partitions they currently own, tagged
    // with the generation those partitions were assigned under (S3), so the
    // leader can preserve stickiness and resolve dual-claims. Eager assigners
    // ignore these extra args (red-team N2).
    const ownedPartitions = this.ownedPartitions()
    const ownedGenerationId = this.ownedGenerationId

    const groupData = await this.coordinator.joinGroup({
      groupId,
      sessionTimeout,
      rebalanceTimeout,
      memberId: this.memberId || '',
      groupProtocols: this.assigners.map(assigner =>
        assigner.protocol({
          topics: this.topicsSubscribed,
          ownedPartitions,
          generationId: ownedGenerationId,
        })
      ),
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

  async [PRIVATE.SYNC]() {
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

    const decodedMemberAssignment = MemberAssignment.decode(memberAssignment)
    const decodedAssignment =
      decodedMemberAssignment != null ? decodedMemberAssignment.assignment : {}

    this.logger.debug('Received assignment', {
      groupId,
      generationId,
      memberId,
      memberAssignment: decodedAssignment,
    })

    const assignedTopics = keys(decodedAssignment)
    const topicsNotSubscribed = arrayDiff(assignedTopics, topicsSubscribed)

    if (topicsNotSubscribed.length > 0) {
      const payload = {
        groupId,
        generationId,
        memberId,
        assignedTopics,
        topicsSubscribed,
        topicsNotSubscribed,
      }

      this.instrumentationEmitter.emit(RECEIVED_UNSUBSCRIBED_TOPICS, payload)
      this.logger.warn('Consumer group received unsubscribed topics', {
        ...payload,
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

        // If the consumer is not aware of all assigned partitions, refresh metadata
        // and update the list of partitions per subscribed topic. It's enough to perform
        // this operation once since refresh metadata will update metadata for all topics
        await this.cluster.refreshMetadata()
        this.partitionsPerSubscribedTopic = this.generatePartitionsPerSubscribedTopic()
        break
      }
    }

    this.topics = currentMemberAssignment.map(({ topic }) => topic)

    const newAssignmentByTopic = currentMemberAssignment.reduce(
      (partitionsByTopic, { topic, partitions }) => ({
        ...partitionsByTopic,
        [topic]: partitions,
      }),
      {}
    )

    if (this.isCooperative() && this.offsetManager) {
      await this.applyCooperativeAssignment({
        currentMemberAssignment,
        newAssignmentByTopic,
        generationId,
        memberId,
        coordinator,
      })
    } else {
      this.subscriptionState.assign(currentMemberAssignment)
      this.offsetManager = new OffsetManager({
        cluster: this.cluster,
        topicConfigurations: this.topicConfigurations,
        instrumentationEmitter: this.instrumentationEmitter,
        memberAssignment: newAssignmentByTopic,
        autoCommit: this.autoCommit,
        autoCommitInterval: this.autoCommitInterval,
        autoCommitThreshold: this.autoCommitThreshold,
        coordinator,
        groupId,
        generationId,
        memberId,
      })

      // On the first cooperative SYNC there is nothing to retain, but we still
      // record the generation our partitions are now owned under (S3) and clear
      // any rejoin signal.
      if (this.isCooperative()) {
        this.ownedGenerationId = generationId
        this.rejoinNeeded = false
        this.cooperativeRounds = 0
      }
    }
  }

  /**
   * Cooperative SYNC branch. Unlike the eager path (which rebuilds the
   * OffsetManager and drops all partitions), this:
   *   1. computes revoked = owned − newAssigned and added = newAssigned − owned
   *   2. re-points the in-place OffsetManager at the current generation, then
   *      commits revoked-partition offsets before dropping them — JoinGroup +
   *      SyncGroup already advanced the generation, so a commit under the old
   *      one would throw ILLEGAL_GENERATION (red-team S6)
   *   3. mutates the OffsetManager in place so retained partitions keep their
   *      resolved-but-uncommitted offsets (red-team F1)
   *   4. updates subscriptionState and records the generation we now own under
   *   5. sets `rejoinNeeded` when partitions were revoked, so the runner runs a
   *      second round and the freed partitions get assigned (red-team S1/S2)
   *
   * @private
   */
  async applyCooperativeAssignment({
    currentMemberAssignment,
    newAssignmentByTopic,
    generationId,
    memberId,
    coordinator,
  }) {
    // `rejoinNeeded` is still true here only when this SYNC is the continuation
    // of an in-flight cooperative rejoin cycle. If it is false, this is a fresh
    // (natural) rebalance, so the round counter starts over — otherwise rounds
    // would accumulate across logically separate rebalances and trip the cap
    // prematurely under sustained churn.
    if (!this.rejoinNeeded) {
      this.cooperativeRounds = 0
    }

    const previousAssigned = this.subscriptionState.assigned()
    const ownedKeys = new Set()
    const ownedByKey = new Map()
    for (const { topic, partitions } of previousAssigned) {
      for (const partition of partitions) {
        const key = `${topic}:${partition}`
        ownedKeys.add(key)
        ownedByKey.set(key, { topic, partition })
      }
    }

    const newKeys = new Set()
    const added = []
    for (const { topic, partitions } of currentMemberAssignment) {
      for (const partition of partitions) {
        const key = `${topic}:${partition}`
        newKeys.add(key)
        if (!ownedKeys.has(key)) added.push({ topic, partition })
      }
    }

    const revoked = []
    for (const key of ownedKeys) {
      if (!newKeys.has(key)) revoked.push(ownedByKey.get(key))
    }

    // The OffsetManager is mutated in place (not rebuilt), so it still carries
    // the PREVIOUS generation. We have already completed JoinGroup + SyncGroup
    // at the new generation, so the broker now expects commits under it — both
    // for the revoked-partition commit below and for future autocommits of
    // retained partitions. Sync the manager's protocol identity to the current
    // generation before any commit, or every commit fails ILLEGAL_GENERATION.
    this.offsetManager.generationId = generationId
    this.offsetManager.memberId = memberId
    this.offsetManager.coordinator = coordinator

    // Commit revoked offsets before dropping them from the manager. This now
    // uses the current (valid) generation set just above (red-team S6: the
    // commit must carry a generation the broker still accepts).
    if (revoked.length > 0) {
      await this.commitRevokedOffsets(revoked)
    }

    this.subscriptionState.assign(currentMemberAssignment)
    this.offsetManager.updateAssignment({ added, revoked })

    this.ownedGenerationId = generationId
    this.lastRevoked = revoked
    this.lastAdded = added

    if (revoked.length > 0 && this.cooperativeRounds < MAX_COOPERATIVE_ROUNDS) {
      this.rejoinNeeded = true
      this.cooperativeRounds += 1
    } else {
      if (revoked.length > 0) {
        // Degraded path: we revoked partitions but hit the round cap. Commit
        // current offsets and warn loudly, then stop looping (red-team S5).
        // The revoked partitions stay unowned until the next natural rebalance
        // re-assigns them — a bounded, recoverable gap, preferred over an
        // unbounded rejoin loop. Normal KIP-429 converges in 2 rounds, so the
        // cap has headroom and this path is not expected in steady state.
        this.logger.warn('Cooperative rebalance did not converge within the round cap', {
          groupId: this.groupId,
          memberId,
          generationId,
          rounds: this.cooperativeRounds,
          revoked,
        })
        await this.offsetManager.commitOffsets()
      }
      this.rejoinNeeded = false
      this.cooperativeRounds = 0
    }
  }

  /**
   * Commit offsets for revoked partitions using the still-valid (pre-rebuild)
   * OffsetManager so the commit carries the generation those partitions were
   * owned under (red-team S6). Only resolved offsets for the revoked partitions
   * are committed; retained partitions are left untouched.
   *
   * @private
   * @param {Array<{topic: string, partition: number}>} revoked
   */
  async commitRevokedOffsets(revoked) {
    const { topics } = this.offsetManager.uncommittedOffsets()
    const revokedSet = new Set(revoked.map(({ topic, partition }) => `${topic}:${partition}`))

    const revokedTopics = topics
      .map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.filter(({ partition }) => revokedSet.has(`${topic}:${partition}`)),
      }))
      .filter(({ partitions }) => partitions.length > 0)

    if (revokedTopics.length === 0) return

    try {
      await this.offsetManager.commitOffsets({ topics: revokedTopics })
    } catch (e) {
      this.logger.warn('Failed to commit revoked partition offsets during cooperative rebalance', {
        groupId: this.groupId,
        memberId: this.memberId,
        error: e.message,
      })
    }
  }

  /**
   * Join the group and sync the assignment. Concurrent callers (e.g. multiple
   * fetchers recovering from stale metadata) share a single in-flight execution
   * so the cooperative SYNC branch never interleaves.
   */
  joinAndSync() {
    return this[PRIVATE.SHARED_JOIN_AND_SYNC]()
  }

  joinAndSyncOnce() {
    const startJoin = Date.now()
    return this.retrier(async bail => {
      try {
        await this[PRIVATE.JOIN]()
        await this[PRIVATE.SYNC]()

        const memberAssignment = this.assigned().reduce(
          (result, { topic, partitions }) => ({ ...result, [topic]: partitions }),
          {}
        )

        const payload = {
          groupId: this.groupId,
          memberId: this.memberId,
          leaderId: this.leaderId,
          isLeader: this.isLeader(),
          memberAssignment,
          groupProtocol: this.groupProtocol,
          duration: Date.now() - startJoin,
          // Cooperative rebalancing surfaces what moved this round; empty for
          // eager assigners so existing GROUP_JOIN listeners are unaffected.
          revoked: this.isCooperative() ? this.lastRevoked : [],
          added: this.isCooperative() ? this.lastAdded : [],
        }

        this.instrumentationEmitter.emit(GROUP_JOIN, payload)
        this.logger.info('Consumer has joined the group', payload)
      } catch (e) {
        if (isRebalancing(e)) {
          // Rebalance in progress isn't a retriable protocol error since the consumer
          // has to go through find coordinator and join again before it can
          // actually retry the operation. We wrap the original error in a retriable error
          // here instead in order to restart the join + sync sequence using the retrier.
          throw new KafkaJSError(e)
        }

        if (e.type === 'UNKNOWN_MEMBER_ID') {
          this.memberId = null
          throw new KafkaJSError(e)
        }

        bail(e)
      }
    })
  }

  /**
   * @param {import("../../types").TopicPartition} topicPartition
   */
  resetOffset({ topic, partition }) {
    this.offsetManager.resetOffset({ topic, partition })
  }

  /**
   * @param {import("../../types").TopicPartitionOffset} topicPartitionOffset
   */
  resolveOffset({ topic, partition, offset }) {
    this.offsetManager.resolveOffset({ topic, partition, offset })
  }

  /**
   * Update the consumer offset for the given topic/partition. This will be used
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
    return this[PRIVATE.SHARED_HEARTBEAT]({ interval })
  }

  async fetch(nodeId) {
    try {
      await this.cluster.refreshMetadataIfNecessary()
      this.checkForStaleAssignment()

      let topicPartitions = this.subscriptionState.assigned()
      topicPartitions = this.filterPartitionsByNode(nodeId, topicPartitions)

      await this.seekOffsets(topicPartitions)

      const committedOffsets = this.offsetManager.committedOffsets()
      const activeTopicPartitions = this.getActiveTopicPartitions()

      const requests = topicPartitions
        .map(({ topic, partitions }) => ({
          topic,
          partitions: partitions
            .filter(
              partition =>
                /**
                 * When recovering from OffsetOutOfRange, each partition can recover
                 * concurrently, which invalidates resolved and committed offsets as part
                 * of the recovery mechanism (see OffsetManager.clearOffsets). In concurrent
                 * scenarios this can initiate a new fetch with invalid offsets.
                 *
                 * This was further highlighted by https://github.com/tulios/kafkajs/pull/570,
                 * which increased concurrency, making this more likely to happen.
                 *
                 * This is solved by only making requests for partitions with initialized offsets.
                 *
                 * See the following pull request which explains the context of the problem:
                 * @issue https://github.com/tulios/kafkajs/pull/578
                 */
                committedOffsets[topic][partition] != null &&
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
                  groupId: this.groupId,
                  memberId: this.memberId,
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
        groupId: this.groupId,
        memberId: this.memberId,
        error: e.message,
      })

      await this.cluster.refreshMetadata()
      await this.joinAndSync()
      return
    }

    if (e.name === 'KafkaJSStaleTopicMetadataAssignment') {
      this.logger.warn(`${e.message}, resync group`, {
        groupId: this.groupId,
        memberId: this.memberId,
        topic: e.topic,
        unknownPartitions: e.unknownPartitions,
      })

      await this.joinAndSync()
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
        groupId: this.groupId,
        memberId: this.memberId,
      })
      delete preferredReadReplicas[e.partition]
    } else {
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
          groupId: this.groupId,
          memberId: this.memberId,
          seek: seekEntry,
        })
        await this.offsetManager.seek(seekEntry)
      }
    }

    await this.offsetManager.resolveOffsets()
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
            groupId: this.groupId,
            memberId: this.memberId,
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
              groupId: this.groupId,
              memberId: this.memberId,
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
    const activeSubscriptionState = this.subscriptionState.active()

    const activeTopicPartitions = {}
    activeSubscriptionState.forEach(({ topic, partitions }) => {
      activeTopicPartitions[topic] = new Set(partitions)
    })

    return activeTopicPartitions
  }
}
