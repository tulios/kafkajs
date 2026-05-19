const OffsetManager = require('../index')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')

describe('Consumer > OffsetManager > getCoordinator', () => {
  let offsetManager, mockCluster, mockCoordinator, mockNewCoordinator, groupId

  beforeEach(() => {
    groupId = 'test-group-id'

    mockCoordinator = {
      isConnected: jest.fn(() => true),
      nodeId: 1,
    }

    mockNewCoordinator = {
      isConnected: jest.fn(() => true),
      nodeId: 2,
    }

    mockCluster = {
      committedOffsets: jest.fn(() => ({})),
      findGroupCoordinator: jest.fn(() => mockNewCoordinator),
    }

    offsetManager = new OffsetManager({
      cluster: mockCluster,
      coordinator: mockCoordinator,
      memberAssignment: { topic1: [0] },
      groupId,
      generationId: 1,
      memberId: 'member-1',
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })
  })

  it('returns the current coordinator when it is connected', async () => {
    const coordinator = await offsetManager.getCoordinator()
    expect(coordinator).toBe(mockCoordinator)
    expect(mockCluster.findGroupCoordinator).not.toHaveBeenCalled()
  })

  it('discovers the new coordinator via findGroupCoordinator when disconnected', async () => {
    mockCoordinator.isConnected.mockReturnValue(false)
    const coordinator = await offsetManager.getCoordinator()
    expect(mockCluster.findGroupCoordinator).toHaveBeenCalledWith({ groupId: 'test-group-id' })
    expect(coordinator).toBe(mockNewCoordinator)
  })

  it('handles coordinator change after broker failover (stale coordinator scenario)', async () => {
    // Simulate: cached coordinator is broker-5 but actual coordinator is broker-2.
    // findBroker(stale) would have returned the same stale broker; findGroupCoordinator
    // asks the cluster which broker is currently authoritative for this group.
    const staleBroker = { isConnected: jest.fn(() => false), nodeId: 5 }
    const correctBroker = { isConnected: jest.fn(() => true), nodeId: 2 }

    offsetManager.coordinator = staleBroker
    mockCluster.findGroupCoordinator.mockResolvedValue(correctBroker)

    const coordinator = await offsetManager.getCoordinator()

    expect(mockCluster.findGroupCoordinator).toHaveBeenCalledWith({ groupId: 'test-group-id' })
    expect(coordinator).toBe(correctBroker)
    expect(coordinator.nodeId).toBe(2)
  })
})
