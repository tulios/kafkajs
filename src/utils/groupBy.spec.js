const groupBy = require('./groupBy')

describe('Utils > groupBy', () => {
  it('group items by the function return', async () => {
    const input = [1, 2, 3, 4]
    const groupFn = item => (item % 2 === 0 ? 'even' : 'odd')
    const output = new Map([
      ['even', [2, 4]],
      ['odd', [1, 3]],
    ])

    await expect(groupBy(input, groupFn)).resolves.toEqual(output)
  })

  it('works with async functions', async () => {
    const input = [1, 2, 3, 4]
    const groupFn = async item => (item % 2 === 0 ? 'even' : 'odd')
    const output = new Map([
      ['even', [2, 4]],
      ['odd', [1, 3]],
    ])

    await expect(groupBy(input, groupFn)).resolves.toEqual(output)
  })

  it('works with objects as group keys', async () => {
    const even = {}
    const odd = {}

    const input = [1, 2, 3, 4]
    const groupFn = async item => (item % 2 === 0 ? even : odd)
    const output = new Map([
      [even, [2, 4]],
      [odd, [1, 3]],
    ])

    await expect(groupBy(input, groupFn)).resolves.toEqual(output)
  })
})
