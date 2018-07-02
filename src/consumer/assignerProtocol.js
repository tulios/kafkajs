const Encoder = require('../protocol/encoder')
const Decoder = require('../protocol/decoder')

const MemberMetadata = {
  /**
   * @param {number} version
   * @param {Array<string>} topics
   * @param {Buffer} [userData=Buffer.alloc(0)]
   *
   * @returns Buffer
   */
  encode({ version, topics, userData = Buffer.alloc(0) }) {
    return new Encoder()
      .writeInt16(version)
      .writeArray(topics)
      .writeBytes(userData).buffer
  },

  /**
   * @param {Buffer} buffer
   * @returns {Object}
   */
  decode(buffer) {
    const decoder = new Decoder(buffer)
    return {
      version: decoder.readInt16(),
      topics: decoder.readArray(d => d.readString()),
      userData: decoder.readBytes(),
    }
  },
}

const MemberAssignment = {
  /**
   * @param {number} version
   * @param {Object<String,Array>} assignment, example:
   *                               {
   *                                 'topic-A': [0, 2, 4, 6],
   *                                 'topic-B': [0, 2],
   *                               }
   * @param {Buffer} [userData=Buffer.alloc(0)]
   *
   * @returns Buffer
   */
  encode({ version, assignment, userData = Buffer.alloc(0) }) {
    return new Encoder()
      .writeInt16(version)
      .writeArray(
        Object.keys(assignment).map(topic =>
          new Encoder().writeString(topic).writeArray(assignment[topic])
        )
      )
      .writeBytes(userData).buffer
  },

  /**
   * @param {Buffer} buffer
   * @returns {Object}
   */
  decode(buffer) {
    const decoder = new Decoder(buffer)
    const decodePartitions = d => d.readInt32()
    const decodeAssignment = d => ({
      topic: d.readString(),
      partitions: d.readArray(decodePartitions),
    })
    const indexAssignment = (obj, { topic, partitions }) =>
      Object.assign(obj, { [topic]: partitions })

    return {
      version: decoder.readInt16(),
      assignment: decoder.readArray(decodeAssignment).reduce(indexAssignment, {}),
      userData: decoder.readBytes(),
    }
  },
}

module.exports = {
  MemberMetadata,
  MemberAssignment,
}
