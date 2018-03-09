const SubscriptionState = require('./subscriptionState')

describe('Consumer > OffsetMananger > pause / resume', () => {
  let subscriptionState

  beforeEach(() => {
    subscriptionState = new SubscriptionState()
  })

  it('pauses the selected topics', () => {
    subscriptionState.pause(['topic1', 'topic2'])
    expect(subscriptionState.paused()).toEqual(['topic1', 'topic2'])
  })

  it('resumes the selected topics', () => {
    subscriptionState.pause(['topic1', 'topic2'])
    subscriptionState.resume(['topic2'])
    expect(subscriptionState.paused()).toEqual(['topic1'])
  })
})
