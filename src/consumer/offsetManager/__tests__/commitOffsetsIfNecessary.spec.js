const Long = require('../../../utils/long')
const sleep = require('../../../utils/sleep')
const OffsetManager = require('../index')

describe('Consumer > OffsetMananger > commitOffsetsIfNecessary', () => {
  let offsetManager, mockCommitOffsets

  beforeEach(() => {
    const memberAssignment = {
      topic1: [0, 1, 2, 3],
      topic2: [0, 1, 2, 3, 4, 5],
    }

    offsetManager = new OffsetManager({
      memberAssignment,
      cluster: {
        committedOffsets: jest.fn(() => ({})),
      },
    })
    offsetManager.commitOffsets = jest.fn()
    offsetManager.committedOffsets()['topic1'][0] = '-1'

    mockCommitOffsets = () => {
      const committedOffsets = offsetManager.committedOffsets()

      for (const topic in offsetManager.resolvedOffsets) {
        committedOffsets[topic] = {}
        for (const partition in offsetManager.resolvedOffsets[topic]) {
          committedOffsets[topic][partition] = offsetManager.resolvedOffsets[topic][partition]
        }
      }
      offsetManager.lastCommit = Date.now()
    }
  })

  describe('when autoCommitInterval and autoCommitThreshold are undefined', () => {
    it('does not commit offsets', async () => {
      offsetManager.autoCommitInterval = undefined
      offsetManager.autoCommitThreshold = undefined
      offsetManager.countResolvedOffsets = jest.fn(() => Long.fromValue(0))

      await offsetManager.commitOffsetsIfNecessary()
      expect(offsetManager.commitOffsets).not.toHaveBeenCalled()
    })
  })

  describe('when autoCommitInterval is defined', () => {
    it('commits the offsets whenever the interval is reached', async () => {
      offsetManager.autoCommitInterval = 30
      offsetManager.autoCommitThreshold = undefined
      offsetManager.countResolvedOffsets = jest.fn(() => Long.fromValue(0))
      offsetManager.commitOffsets.mockImplementation(() => {
        offsetManager.lastCommit = Date.now()
      })

      await offsetManager.commitOffsetsIfNecessary()
      await sleep(50)

      await offsetManager.commitOffsetsIfNecessary()
      await offsetManager.commitOffsetsIfNecessary()

      expect(offsetManager.commitOffsets).toHaveBeenCalledTimes(1)
    })
  })

  describe('when autoCommitThreshold is defined', () => {
    it('commits the offsets whenever the threshold is reached', async () => {
      offsetManager.autoCommitInterval = undefined
      offsetManager.autoCommitThreshold = 3
      offsetManager.commitOffsets.mockImplementation(mockCommitOffsets)

      await offsetManager.commitOffsetsIfNecessary()
      offsetManager.resolveOffset({ topic: 'topic1', partition: 0, offset: '3' })

      await offsetManager.commitOffsetsIfNecessary()
      await offsetManager.commitOffsetsIfNecessary()

      expect(offsetManager.commitOffsets).toHaveBeenCalledTimes(1)
    })
  })

  describe('when autoCommitInterval and autoCommitThreshold are defined', () => {
    beforeEach(() => {
      offsetManager.commitOffsets.mockImplementation(mockCommitOffsets)
    })

    it('commits the offsets if the interval is reached before the threshold', async () => {
      offsetManager.autoCommitInterval = 10
      offsetManager.autoCommitThreshold = 25

      offsetManager.resolveOffset({ topic: 'topic1', partition: 0, offset: '3' })
      await sleep(15)

      await offsetManager.commitOffsetsIfNecessary()
      expect(offsetManager.commitOffsets).toHaveBeenCalledTimes(1)
    })

    it('commits the offsets if the threshold is reached before the timeout', async () => {
      offsetManager.autoCommitInterval = 500
      offsetManager.autoCommitThreshold = 2

      offsetManager.resolveOffset({ topic: 'topic1', partition: 0, offset: '3' })
      await sleep(15)

      await offsetManager.commitOffsetsIfNecessary()
      expect(offsetManager.commitOffsets).toHaveBeenCalledTimes(1)
    })
  })
})
