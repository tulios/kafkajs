const SubscriptionState = require('./subscriptionState')

describe('Consumer > OffsetMananger > pause / resume', () => {
  let subscriptionState
  const byTopic = (a, b) => a.topic - b.topic

  beforeEach(() => {
    subscriptionState = new SubscriptionState()
  })

  it('pauses the selected topics', () => {
    subscriptionState.pause([{ topic: 'topic1' }, { topic: 'topic2' }])
    expect(subscriptionState.paused().sort(byTopic)).toEqual([
      { topic: 'topic1', partitions: [], all: true },
      { topic: 'topic2', partitions: [], all: true },
    ])
  })

  it('resumes the selected topics', () => {
    subscriptionState.pause([{ topic: 'topic1' }, { topic: 'topic2' }])
    subscriptionState.resume([{ topic: 'topic2' }])

    expect(subscriptionState.paused().sort(byTopic)).toEqual([
      { topic: 'topic1', partitions: [], all: true },
      { topic: 'topic2', partitions: [], all: false },
    ])
  })

  it('pauses the selected partitions', () => {
    subscriptionState.pause([{ topic: 'topic1', partitions: [0, 1] }])
    expect(subscriptionState.paused().sort(byTopic)).toEqual([
      { topic: 'topic1', partitions: [0, 1], all: false },
    ])

    subscriptionState.pause([{ topic: 'topic1', partitions: [1, 2] }])
    expect(subscriptionState.paused().sort(byTopic)).toEqual([
      { topic: 'topic1', partitions: [0, 1, 2], all: false },
    ])
  })

  it('resumes the selected partitions', () => {
    subscriptionState.pause([{ topic: 'topic1', partitions: [0, 1] }])
    subscriptionState.resume([{ topic: 'topic1', partitions: [1] }])
    expect(subscriptionState.paused().sort(byTopic)).toEqual([
      { topic: 'topic1', partitions: [0], all: false },
    ])

    subscriptionState.resume([{ topic: 'topic1', partitions: [2] }])
    expect(subscriptionState.paused().sort(byTopic)).toEqual([
      { topic: 'topic1', partitions: [0], all: false },
    ])
  })
})

describe('Consumer > OffsetMananger > isPaused', () => {
  let subscriptionState

  beforeEach(() => {
    subscriptionState = new SubscriptionState()
  })

  it('can determine whether a topic partition is paused', () => {
    subscriptionState.pause([{ topic: 'topic1', partitions: [0, 1] }, { topic: 'topic2' }])

    expect(subscriptionState.isPaused('topic1', 0)).toEqual(true)
    expect(subscriptionState.isPaused('topic1', 2)).toEqual(false)
    expect(subscriptionState.isPaused('topic2', 0)).toEqual(true)
    expect(subscriptionState.isPaused('topic2', 2)).toEqual(true)
    expect(subscriptionState.isPaused('unknown', 0)).toEqual(false)
  })
})
