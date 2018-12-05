const ConsumerGroup = require('../consumerGroup')
const { newLogger } = require('testHelpers')

describe('ConsumerGroup', () => {
  let consumerGroup

  beforeEach(() => {
    consumerGroup = new ConsumerGroup({
      logger: newLogger(),
      topics: ['topic1'],
      cluster: {},
    })
  })

  describe('uncommittedOffsets', () => {
    it("calls the offset manager's uncommittedOffsets", async () => {
      const mockOffsets = { topics: [] }
      consumerGroup.offsetManager = { uncommittedOffsets: jest.fn(() => mockOffsets) }

      expect(consumerGroup.uncommittedOffsets()).toStrictEqual(mockOffsets)
      expect(consumerGroup.offsetManager.uncommittedOffsets).toHaveBeenCalled()
    })
  })
})
