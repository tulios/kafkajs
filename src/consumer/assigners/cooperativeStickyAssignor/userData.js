const Encoder = require('../../../protocol/encoder')
const Decoder = require('../../../protocol/decoder')

/**
 * Generation used when a member has not yet been assigned partitions under a
 * known generation (mirrors Java's DEFAULT_GENERATION).
 */
const DEFAULT_GENERATION = -1

/**
 * Current codec version for the cooperative-sticky userData layout.
 */
const USER_DATA_VERSION = 0

/**
 * Codec for the cooperative-sticky assigner `userData` buffer.
 *
 * KafkaJS uses its own member-metadata schema (version/topics/userData) and has
 * no native `ownedPartitions` wire field like Java's ConsumerProtocolSubscription.
 * We therefore encode both the partitions a member currently owns and the
 * generation they were assigned under into `userData`.
 *
 * Wire layout (big-endian, via Encoder):
 *   version:         int16  (codec version, starts at 0)
 *   generationId:    int32  (-1 when unknown)
 *   ownedPartitions: array of { topic: string, partitions: array<int32> }
 *
 * This layout is internal to KafkaJS cooperative-sticky members; it is not
 * compatible with Java consumers (same interop limitation as roundRobin).
 */
const UserData = {
  /**
   * @param {Object} data
   * @param {number} [data.version=0]
   * @param {number} [data.generationId=-1]
   * @param {Object<string, number[]>} [data.ownedPartitions={}] e.g. { 'topic-A': [0, 2] }
   * @returns {Buffer}
   */
  encode({ version = USER_DATA_VERSION, generationId = DEFAULT_GENERATION, ownedPartitions = {} }) {
    const sortedTopics = Object.keys(ownedPartitions).sort()

    return new Encoder()
      .writeInt16(version)
      .writeInt32(generationId)
      .writeArray(
        sortedTopics.map(topic =>
          new Encoder().writeString(topic).writeArray(
            [...ownedPartitions[topic]].sort((a, b) => a - b),
            'int32'
          )
        ),
        'object'
      ).buffer
  },

  /**
   * Backward-tolerant decode. A non-cooperative or empty `userData` buffer
   * (e.g. roundRobin's empty bytes) decodes to defaults without throwing.
   *
   * @param {Buffer} buffer
   * @returns {{ version: number, generationId: number, ownedPartitions: Object<string, number[]> }}
   */
  decode(buffer) {
    const defaults = {
      version: USER_DATA_VERSION,
      generationId: DEFAULT_GENERATION,
      ownedPartitions: {},
    }

    if (!buffer || buffer.length === 0) {
      return defaults
    }

    const decoder = new Decoder(buffer)

    if (!decoder.canReadInt16()) {
      return defaults
    }

    const version = decoder.readInt16()

    if (!decoder.canReadInt32()) {
      return defaults
    }

    const generationId = decoder.readInt32()
    const ownedPartitions = decoder
      .readArray(d => ({
        topic: d.readString(),
        partitions: d.readArray(p => p.readInt32()),
      }))
      .reduce((acc, { topic, partitions }) => {
        acc[topic] = partitions
        return acc
      }, {})

    return { version, generationId, ownedPartitions }
  },
}

module.exports = {
  UserData,
  DEFAULT_GENERATION,
  USER_DATA_VERSION,
}
