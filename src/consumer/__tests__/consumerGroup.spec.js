const Consumer = require('../consumer')
const { newLogger } = require('testHelpers')

describe('Consumer', () => {
  let consumer

  beforeEach(() => {
    consumer = new Consumer({
      groupId: 'test-consumer',
      logger: newLogger(),
      topics: ['topic1'],
      cluster: {},
    })
  })

  describe('uncommittedOffsets', () => {
    it("calls the offset manager's uncommittedOffsets", async () => {
      const mockOffsets = { topics: [] }
      consumer.offsetManager = { uncommittedOffsets: jest.fn(() => mockOffsets) }

      expect(consumer.uncommittedOffsets()).toStrictEqual(mockOffsets)
      expect(consumer.offsetManager.uncommittedOffsets).toHaveBeenCalled()
    })
  })

  describe('commitOffsets', () => {
    it("calls the offset manager's commitOffsets", async () => {
      consumer.offsetManager = { commitOffsets: jest.fn(() => Promise.resolve()) }

      const offsets = { topics: [{ partitions: [{ offset: '0', partition: 0 }] }] }
      await consumer.commitOffsets(offsets)
      expect(consumer.offsetManager.commitOffsets).toHaveBeenCalledTimes(1)
      expect(consumer.offsetManager.commitOffsets).toHaveBeenCalledWith(offsets)
    })
  })
})
