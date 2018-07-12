const isInvalidOffset = require('../isInvalidOffset')

describe('Consumer > OffsetMananger > isInvalidOffset', () => {
  it('returns true for negative offsets', () => {
    expect(isInvalidOffset(-1)).toEqual(true)
    expect(isInvalidOffset('-1')).toEqual(true)
    expect(isInvalidOffset(-2)).toEqual(true)
    expect(isInvalidOffset('-3')).toEqual(true)
  })

  it('returns true for blank values', () => {
    expect(isInvalidOffset(null)).toEqual(true)
    expect(isInvalidOffset(undefined)).toEqual(true)
    expect(isInvalidOffset('')).toEqual(true)
  })

  it('returns false for positive offsets', () => {
    expect(isInvalidOffset(0)).toEqual(false)
    expect(isInvalidOffset(1)).toEqual(false)
    expect(isInvalidOffset('2')).toEqual(false)
    expect(isInvalidOffset('3')).toEqual(false)
  })
})
