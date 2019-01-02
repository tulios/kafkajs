const swapObject = require('./swapObject')

describe('Utils > swapObject', () => {
  it('swaps keys with values', () => {
    const obj = {
      a1: 'a2',
      b1: 'b2',
      c1: 'c2',
    }

    expect(swapObject(obj)).toEqual({
      a2: 'a1',
      b2: 'b1',
      c2: 'c1',
    })
  })
})
