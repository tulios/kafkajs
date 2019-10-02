jest.setTimeout(1000)

const sleep = require('./sleep')
const BufferedAsyncIterator = require('./bufferedAsyncIterator')

describe('Utils > BufferedAsyncIterator', () => {
  it('provide the values as they become available', async () => {
    const promises = [sleep(300).then(() => 1), sleep(100).then(() => 2), sleep(500).then(() => 3)]
    const iterator = BufferedAsyncIterator(promises)
    const testResults = []

    while (true) {
      const result = iterator.next()
      if (result.done) {
        break
      }

      const value = await result.value
      testResults.push(value)
    }

    expect(testResults).toEqual([2, 1, 3])
  })

  it('does not lose values even if they finish at the "same" time', async () => {
    const promises = [sleep(100).then(() => 1), sleep(100).then(() => 2), sleep(100).then(() => 3)]
    const iterator = BufferedAsyncIterator(promises)
    const testResults = []

    while (true) {
      const result = iterator.next()
      if (result.done) {
        break
      }

      const value = await result.value
      testResults.push(value)
    }

    expect(testResults).toEqual([1, 2, 3])
  })

  it('works with eager consumers', async () => {
    const promises = [sleep(300).then(() => 1), sleep(100).then(() => 2), sleep(500).then(() => 3)]
    const iterator = BufferedAsyncIterator(promises)
    const testResults = []

    const firstResult = await Promise.all([iterator.next().value, iterator.next().value])
    testResults.push(...firstResult)

    const value = await iterator.next().value
    testResults.push(value)

    expect(testResults).toEqual([2, 1, 3])
  })
})
