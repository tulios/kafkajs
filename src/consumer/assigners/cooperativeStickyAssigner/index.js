const Encoder = require('../../../protocol/encoder')
const Decoder = require('../../../protocol/decoder')
const { MemberMetadata, MemberAssignment } = require('../../assignerProtocol')

const DEFAULT_GENERATION = -1
const CONTESTED = 'CONTESTED'

/**
 * Encodes generationId and ownedPartitions into a userData Buffer.
 *
 * Wire format:
 *   INT32   generationId
 *   INT32   numTopics
 *   for each topic:
 *     STRING  topicName   (INT16 length prefix + UTF-8 bytes)
 *     INT32   numPartitions
 *     for each partition:
 *       INT32   partitionId
 *
 * @param {number} generationId
 * @param {Object<string, number[]>} ownedPartitions
 * @returns {Buffer}
 */
function encodeUserData(generationId, ownedPartitions) {
  return new Encoder()
    .writeInt32(generationId)
    .writeArray(
      Object.entries(ownedPartitions).map(([topic, partitions]) =>
        new Encoder().writeString(topic).writeArray(partitions)
      )
    ).buffer
}

/**
 * Decodes a userData Buffer produced by encodeUserData.
 *
 * @param {Buffer|null} buffer
 * @returns {{ generationId: number, ownedPartitions: Object<string, number[]> }}
 */
function decodeUserData(buffer) {
  if (!buffer || buffer.length === 0) {
    return { generationId: DEFAULT_GENERATION, ownedPartitions: {} }
  }
  try {
    const decoder = new Decoder(buffer)
    const generationId = decoder.readInt32()
    const entries = decoder.readArray(d => ({
      topic: d.readString(),
      partitions: d.readArray(pd => pd.readInt32()),
    }))
    const ownedPartitions = entries.reduce(
      (acc, { topic, partitions }) => ({ ...acc, [topic]: partitions }),
      {}
    )
    return { generationId, ownedPartitions }
  } catch (_) {
    return { generationId: DEFAULT_GENERATION, ownedPartitions: {} }
  }
}

/**
 * CooperativeStickyAssigner
 *
 * Implements the cooperative sticky partition assignment strategy, mirroring Kafka's
 * CooperativeStickyAssignor (KIP-429).
 *
 * Two key properties:
 *  1. Sticky      – existing partition→consumer mappings are preserved whenever possible,
 *                   minimising unnecessary partition movement.
 *  2. Cooperative – only partitions that *change ownership* are withheld from the new
 *                   owner's current assignment; they will be handed over in the next
 *                   rebalance after the previous owner has revoked them.  All other
 *                   partitions remain assigned and consumers continue processing
 *                   uninterrupted.
 *
 * Algorithm (mirrors Java AbstractStickyAssignor + CooperativeStickyAssignor):
 *
 *  Step 1 – Parse metadata
 *    Each member encodes its generationId and ownedPartitions into userData.  The leader
 *    decodes all members' metadata to reconstruct the previous ownership map.
 *
 *  Step 2 – Resolve ownership conflicts
 *    If two members in the *same* generation both claim a partition it is treated as
 *    unowned (contested).  If they are in *different* generations the member with the
 *    higher generationId wins.
 *
 *  Step 3 – Sticky assignment
 *    Every partition whose previous owner is still present and subscribed is assigned
 *    back to that owner.  Unresolvable or orphaned partitions go into the unassigned pool.
 *
 *  Step 4 – Quota-aware balancing (ConstrainedAssignmentBuilder, KIP-429)
 *    Compute minQuota = floor(P/M) and maxQuota = ceil(P/M), where P = total partitions
 *    and M = number of members.  Only (P % M) members may keep maxQuota partitions;
 *    the rest must be trimmed to minQuota.  Members are evaluated in sorted order — the
 *    first (P % M) members above minQuota fill the maxQuota slots; subsequent members
 *    have excess partitions moved to the unassigned pool.  Unassigned partitions are
 *    then distributed round-robin, first to members below minQuota, then to those
 *    below maxQuota.
 *
 *  Step 5 – Cooperative adjustment
 *    For every partition that is BOTH (a) newly assigned to its receiver AND (b) revoked
 *    from its previous owner, remove it from the receiver's assignment this round.  The
 *    receiver will obtain it in the next rebalance once the previous owner has stopped
 *    consuming it.  Partitions that are new (never previously owned) or that stay with
 *    the same consumer are NOT withheld.
 *
 *  Step 6 – Encode and return.
 *
 * The cooperative round-2 mechanism is handled by ConsumerGroup.joinAndSync(): if
 * onAssignment() detects revocations it sets needsCooperativeRejoin = true, and
 * joinAndSync() immediately performs another JOIN + SYNC cycle to complete the transfer.
 *
 * @type {import('types').PartitionAssigner}
 */
module.exports = ({ cluster }) => {
  // Per-instance state updated via onAssignment after every successful sync.
  let currentGenerationId = DEFAULT_GENERATION
  let currentOwnedPartitions = {} // { [topic]: number[] }

  return {
    name: 'CooperativeStickyAssigner',
    version: 1,

    /**
     * Called by ConsumerGroup after a successful sync so that the assigner can
     * remember what partitions this consumer currently owns. That information is
     * encoded into the next protocol() call so that the group leader can perform
     * sticky + cooperative assignment.
     *
     * Returns true if any previously-owned partition is absent from the new assignment,
     * which signals to the runner that a cooperative round-2 rejoin is needed so the
     * revoked partitions can be handed to their new owners in the next rebalance.
     *
     * @param {object} options
     * @param {Object<string, number[]>} options.assignment  topic → partition IDs
     * @param {number}                  options.generationId
     * @returns {boolean} true if a cooperative round-2 rejoin should be triggered
     */
    onAssignment({ assignment, generationId }) {
      const hadRevocations =
        currentGenerationId !== DEFAULT_GENERATION &&
        Object.entries(currentOwnedPartitions).some(([topic, parts]) =>
          parts.some(p => !(assignment[topic] || []).includes(p))
        )
      currentGenerationId = generationId
      currentOwnedPartitions = assignment
      return hadRevocations
    },

    /**
     * Perform cooperative sticky assignment.
     *
     * @param {object}                       group
     * @param {import('types').GroupMember[]} group.members
     * @param {string[]}                      group.topics
     * @returns {Promise<import('types').GroupMemberAssignment[]>}
     */
    async assign({ members, topics }) {
      // ── Step 1: parse member metadata ──────────────────────────────────────
      const sortedMembers = members.map(m => m.memberId).sort()
      /** @type {Object<string, { subscribedTopics: string[], generationId: number, ownedPartitions: Object<string, number[]> }>} */
      const memberInfo = {}

      for (const { memberId, memberMetadata } of members) {
        const decoded = MemberMetadata.decode(memberMetadata)
        const { generationId: memberGen, ownedPartitions: memberOwned } = decodeUserData(
          decoded.userData
        )
        memberInfo[memberId] = {
          subscribedTopics: decoded.topics,
          generationId: memberGen,
          ownedPartitions: memberOwned,
        }
      }

      // ── Step 2: collect all partition ids per topic ────────────────────────
      /** @type {Object<string, number[]>} */
      const topicPartitions = {}
      for (const topic of topics) {
        topicPartitions[topic] = cluster
          .findTopicPartitionMetadata(topic)
          .map(m => m.partitionId)
          .sort((a, b) => a - b)
      }

      // ── Step 3: resolve ownership conflicts ────────────────────────────────
      // prevOwner[topic][partitionId] = memberId | CONTESTED | undefined
      /** @type {Object<string, Object<number, string|undefined>>} */
      const prevOwner = {}
      for (const topic of topics) {
        prevOwner[topic] = {}
      }

      for (const memberId of sortedMembers) {
        const {
          ownedPartitions: memberOwned,
          generationId: memberGen,
          subscribedTopics,
        } = memberInfo[memberId]

        for (const [topic, partitions] of Object.entries(memberOwned)) {
          if (!topicPartitions[topic]) continue // topic no longer exists
          if (!subscribedTopics.includes(topic)) continue // member unsubscribed from topic

          for (const partitionId of partitions) {
            if (!topicPartitions[topic].includes(partitionId)) continue // partition gone

            const existing = prevOwner[topic][partitionId]

            if (existing === undefined) {
              prevOwner[topic][partitionId] = memberId
            } else if (existing !== CONTESTED) {
              // Another member already claimed this partition
              const existingGen = memberInfo[existing].generationId
              if (existingGen === memberGen) {
                // Same generation → contested; treat as unowned this round
                prevOwner[topic][partitionId] = CONTESTED
              } else if (memberGen > existingGen) {
                // Current member has a newer generation → it wins
                prevOwner[topic][partitionId] = memberId
              }
              // else: existing member has the newer generation, leave it
            }
            // If already CONTESTED further claims don't change anything
          }
        }
      }

      // ── Step 4: sticky assignment ──────────────────────────────────────────
      /** @type {Object<string, Object<string, number[]>>} */
      const assignment = {}
      for (const memberId of sortedMembers) {
        assignment[memberId] = {}
      }

      const unassigned = [] // { topic, partitionId } pairs that need a new owner

      for (const topic of topics) {
        for (const partitionId of topicPartitions[topic]) {
          const owner = prevOwner[topic][partitionId]

          if (
            owner &&
            owner !== CONTESTED &&
            assignment[owner] !== undefined &&
            memberInfo[owner].subscribedTopics.includes(topic)
          ) {
            if (!assignment[owner][topic]) assignment[owner][topic] = []
            assignment[owner][topic].push(partitionId)
          } else {
            unassigned.push({ topic, partitionId })
          }
        }
      }

      // ── Step 5: balance ────────────────────────────────────────────────────
      const totalPartitions = topics.reduce((sum, topic) => sum + topicPartitions[topic].length, 0)
      const memberCount = sortedMembers.length
      const minQuota = Math.floor(totalPartitions / memberCount)
      const maxQuota = Math.ceil(totalPartitions / memberCount)

      const countPartitions = memberId =>
        Object.values(assignment[memberId]).reduce((sum, parts) => sum + parts.length, 0)

      // Enforce quotas: only `expectedMaxQuotaMembers` members may keep maxQuota
      // partitions; the rest must be trimmed to minQuota.  This mirrors the Java
      // AbstractStickyAssignor's ConstrainedAssignmentBuilder (KIP-429).
      const expectedMaxQuotaMembers = totalPartitions % memberCount
      let currentMaxQuotaMembers = 0

      /** Remove partitions from `memberId` until it has at most `target` */
      const trimTo = (memberId, target) => {
        while (countPartitions(memberId) > target) {
          let revoked = false
          for (const topic of Object.keys(assignment[memberId])) {
            if (assignment[memberId][topic] && assignment[memberId][topic].length > 0) {
              const partitionId = assignment[memberId][topic].pop()
              unassigned.push({ topic, partitionId })
              if (assignment[memberId][topic].length === 0) {
                delete assignment[memberId][topic]
              }
              revoked = true
              break
            }
          }
          if (!revoked) break
        }
      }

      for (const memberId of sortedMembers) {
        // First, unconditionally trim anything above maxQuota
        trimTo(memberId, maxQuota)

        // Then, enforce the maxQuota member limit: only expectedMaxQuotaMembers
        // members may sit above minQuota. The rest must drop to minQuota.
        if (countPartitions(memberId) > minQuota) {
          if (currentMaxQuotaMembers < expectedMaxQuotaMembers) {
            currentMaxQuotaMembers++
          } else {
            trimTo(memberId, minQuota)
          }
        }
      }

      // Distribute unassigned partitions to under-assigned members (round-robin)
      let memberIdx = 0
      for (const { topic, partitionId } of unassigned) {
        let assigned = false

        // First pass: prefer members that are still below minQuota
        for (let i = 0; i < sortedMembers.length; i++) {
          const candidateIdx = (memberIdx + i) % sortedMembers.length
          const candidateId = sortedMembers[candidateIdx]
          if (
            memberInfo[candidateId].subscribedTopics.includes(topic) &&
            countPartitions(candidateId) < minQuota
          ) {
            if (!assignment[candidateId][topic]) assignment[candidateId][topic] = []
            assignment[candidateId][topic].push(partitionId)
            memberIdx = (candidateIdx + 1) % sortedMembers.length
            assigned = true
            break
          }
        }

        if (!assigned) {
          // Second pass: any member below maxQuota
          for (let i = 0; i < sortedMembers.length; i++) {
            const candidateIdx = (memberIdx + i) % sortedMembers.length
            const candidateId = sortedMembers[candidateIdx]
            if (
              memberInfo[candidateId].subscribedTopics.includes(topic) &&
              countPartitions(candidateId) < maxQuota
            ) {
              if (!assignment[candidateId][topic]) assignment[candidateId][topic] = []
              assignment[candidateId][topic].push(partitionId)
              memberIdx = (candidateIdx + 1) % sortedMembers.length
              assigned = true
              break
            }
          }
        }

        if (!assigned) {
          // Last resort: any member subscribed to this topic (handles unequal subscriptions)
          for (let i = 0; i < sortedMembers.length; i++) {
            const candidateIdx = (memberIdx + i) % sortedMembers.length
            const candidateId = sortedMembers[candidateIdx]
            if (memberInfo[candidateId].subscribedTopics.includes(topic)) {
              if (!assignment[candidateId][topic]) assignment[candidateId][topic] = []
              assignment[candidateId][topic].push(partitionId)
              memberIdx = (candidateIdx + 1) % sortedMembers.length
              assigned = true
              break
            }
          }
        }
      }

      // ── Step 6: cooperative adjustment (mirrors Java CooperativeStickyAssignor) ──
      //
      // A partition is a "transfer" if and only if BOTH conditions hold:
      //   (a) It is NEW for the consumer receiving it  (assigned but not in ownedPartitions)
      //   (b) It is LOST by some other consumer       (in ownedPartitions but not assigned)
      //
      // Only transfers are removed from the new owner this round.  Partitions that are
      // fresh (never owned by anyone) or that stay with the same owner are unaffected.
      // This ensures consumers that *keep* their partitions never have to stop processing.

      // Build per-member key sets using "topic:partitionId" strings for fast lookup
      const makeKeySet = obj => {
        const s = new Set()
        for (const [t, parts] of Object.entries(obj)) {
          for (const p of parts) s.add(`${t}:${p}`)
        }
        return s
      }

      /** Maps "topic:partitionId" → memberId for all partitions that are new to their new owner */
      const allAddedPartitions = {}
      /** Set of "topic:partitionId" strings for all partitions lost by their previous owner */
      const allRevokedPartitions = new Set()

      for (const memberId of sortedMembers) {
        const ownedSet = makeKeySet(memberInfo[memberId].ownedPartitions)
        const assignedSet = makeKeySet(assignment[memberId])

        for (const key of assignedSet) {
          if (!ownedSet.has(key)) allAddedPartitions[key] = memberId
        }
        for (const key of ownedSet) {
          if (!assignedSet.has(key)) allRevokedPartitions.add(key)
        }
      }

      // Remove actual transfers from the new owner's assignment for this round
      for (const [key, newOwnerId] of Object.entries(allAddedPartitions)) {
        if (!allRevokedPartitions.has(key)) continue // not a transfer – no previous owner lost it

        const colonIdx = key.indexOf(':')
        const topic = key.slice(0, colonIdx)
        const partitionId = parseInt(key.slice(colonIdx + 1), 10)

        if (assignment[newOwnerId][topic]) {
          const idx = assignment[newOwnerId][topic].indexOf(partitionId)
          if (idx !== -1) {
            assignment[newOwnerId][topic].splice(idx, 1)
            if (assignment[newOwnerId][topic].length === 0) {
              delete assignment[newOwnerId][topic]
            }
          }
        }
      }

      // ── Step 7: encode and return ──────────────────────────────────────────
      return sortedMembers.map(memberId => ({
        memberId,
        memberAssignment: MemberAssignment.encode({
          version: this.version,
          assignment: assignment[memberId],
        }),
      }))
    },

    /**
     * Returns the group protocol metadata for this consumer.
     * Encodes the current generationId and owned partitions into userData so that
     * the group leader can perform sticky + cooperative assignment.
     *
     * @param {{ topics: string[] }} subscription
     * @returns {{ name: string, metadata: Buffer }}
     */
    protocol({ topics }) {
      return {
        name: this.name,
        metadata: MemberMetadata.encode({
          version: this.version,
          topics,
          userData: encodeUserData(currentGenerationId, currentOwnedPartitions),
        }),
      }
    },
  }
}

// Export helpers for testing
module.exports.encodeUserData = encodeUserData
module.exports.decodeUserData = decodeUserData
