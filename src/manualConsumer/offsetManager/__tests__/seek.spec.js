const OffsetManager = require('../index')

describe('Consumer > OffsetMananger > seek', () => {
  let offsetManager, coordinator
  beforeEach(() => {
    const memberAssignment = {
      topic1: [0, 1, 2, 3],
      topic2: [0, 1, 2, 3, 4, 5],
    }

    coordinator = { offsetCommit: jest.fn() }
    offsetManager = new OffsetManager({ memberAssignment })
    offsetManager.getCoordinator = jest.fn(() => coordinator)
  })

  it('ignores the seek when the consumer is not assigned to the topic', async () => {
    await offsetManager.seek({ topic: 'topic3', partition: 0, offset: '100' })
    expect(offsetManager.getCoordinator).not.toHaveBeenCalled()
    expect(coordinator.offsetCommit).not.toHaveBeenCalled()
  })

  it('ignores the seek when the consumer is not assigned to the partition', async () => {
    await offsetManager.seek({ topic: 'topic1', partition: 4, offset: '101' })
    expect(offsetManager.getCoordinator).not.toHaveBeenCalled()
    expect(coordinator.offsetCommit).not.toHaveBeenCalled()
  })
})
