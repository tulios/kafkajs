const arrayDiff = require('./arrayDiff')

describe('Utils > arrayDiff', () => {
  it('returns the elements in A that are not in B', () => {
    const a = [1, 2, 3, 4]
    const b = [2, 3, 4]
    expect(arrayDiff(a, b)).toEqual([1])
  })

  it('takes null and undefined in consideration', () => {
    const a = [1, 2, 3, 4, null, undefined]
    const b = [2, 3, 4, 5]
    expect(arrayDiff(a, b)).toEqual([1, null, undefined])
  })

  it('returns empty if A is empty', () => {
    const b = [2, 3, 4, 5]
    expect(arrayDiff([], b)).toEqual([])
  })

  it('only takes A in consideration', () => {
    const a = [1, 2, 3]
    const b = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    expect(arrayDiff(a, b)).toEqual([])
  })
})
