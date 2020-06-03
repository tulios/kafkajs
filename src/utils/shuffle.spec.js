const shuffle = require('./shuffle')

describe('Utils > shuffle', () => {
  it('shuffles', () => {
    const array = [1, 2, 3]
    const shuffled = shuffle(array)

    expect(shuffled).not.toEqual(array)
    expect(shuffled).toIncludeSameMembers(array)
  })

  it('returns the same order for single element arrays', () => {
    expect(shuffle([1])).toEqual([1])
  })

  it('throws if it receives a non-array', () => {
    expect(() => shuffle()).toThrowError(TypeError)
    expect(() => shuffle('foo')).toThrowError(TypeError)
    expect(() => shuffle({})).toThrowError(TypeError)
  })
})
