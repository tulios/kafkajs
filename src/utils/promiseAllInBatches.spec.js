const promiseAllInBatches = require('./promiseAllInBatches')

describe('Utils > promiseAllInBatches', () => {
  it('executes Promise.all in batches', async () => {
    const promises = [
      () => Promise.resolve(1),
      () => Promise.resolve(2),
      () => Promise.resolve(3),
      () => Promise.resolve(4),
      () => Promise.resolve(5),
      () => Promise.resolve(6),
      () => Promise.resolve(7),
    ]

    const results = await promiseAllInBatches({ batchSize: 2 }, promises)
    expect(results).toEqual([1, 2, 3, 4, 5, 6, 7])
  })
})
