const { isRebalancing } = require('./errors')

describe('isRebalancing', () => {
  it('returns true for REBALANCE_IN_PROGRESS', () => {
    expect(isRebalancing({ type: 'REBALANCE_IN_PROGRESS' })).toBe(true)
  })

  it('returns true for NOT_COORDINATOR_FOR_GROUP', () => {
    expect(isRebalancing({ type: 'NOT_COORDINATOR_FOR_GROUP' })).toBe(true)
  })

  it('returns true for ILLEGAL_GENERATION', () => {
    expect(isRebalancing({ type: 'ILLEGAL_GENERATION' })).toBe(true)
  })

  it('returns true for GROUP_LOAD_IN_PROGRESS', () => {
    expect(isRebalancing({ type: 'GROUP_LOAD_IN_PROGRESS' })).toBe(true)
  })

  it('returns false for unrelated error types', () => {
    expect(isRebalancing({ type: 'UNKNOWN_MEMBER_ID' })).toBe(false)
    expect(isRebalancing({ type: 'NETWORK_EXCEPTION' })).toBe(false)
    expect(isRebalancing({ type: 'BROKER_NOT_AVAILABLE' })).toBe(false)
  })

  it('returns false when error has no type', () => {
    expect(isRebalancing({})).toBe(false)
    expect(isRebalancing({ message: 'something failed' })).toBe(false)
  })
})
