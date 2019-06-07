const OffsetManager = require('../index')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')

describe('Consumer > OffsetMananger > clearTopicOffsets', () => {
  let offsetManager

  beforeEach(async () => {
    const memberAssignment = {
      topic1: [0],
      topic2: [0, 1],
    }

    const groupId = 'groupId'
    const generationId = 'generationId'
    const memberId = 'memberId'

    offsetManager = new OffsetManager({
      cluster: {
        committedOffsets: jest.fn(() => ({})),
      },
      memberAssignment,
      groupId,
      generationId,
      memberId,
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })

    offsetManager.getCoordinator = jest.fn(() => ({
      offsetCommit: jest.fn(),
    }))

    offsetManager.resolveOffset({ topic: 'topic1', partition: 0, offset: '500' })
    offsetManager.resolveOffset({ topic: 'topic2', partition: 0, offset: '600' })
    offsetManager.resolveOffset({ topic: 'topic2', partition: 1, offset: '700' })
    await offsetManager.commitOffsets()
  })

  it('erases all resolved offsets', () => {
    expect(offsetManager.committedOffsets()).toEqual({
      topic1: { '0': '501' },
      topic2: { '0': '601', '1': '701' },
    })

    offsetManager.clearTopicOffsets({ topics: ['topic2'] })

    expect(offsetManager.committedOffsets()).toEqual({
      topic1: { '0': '501' },
      topic2: { '0': undefined, '1': undefined },
    })
  })
})
