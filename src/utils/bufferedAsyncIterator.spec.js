jest.setTimeout(1000)

const sleep = require('./sleep')
const BufferedAsyncIterator = require('./bufferedAsyncIterator')

describe('Utils > BufferedAsyncIterator', () => {
  it('yields an empty iterator if passed an empty array of promises', async () => {
    const iterator = BufferedAsyncIterator([])
    const result = iterator.next()

    expect(result.done).toBe(true)
  })

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

  it('allows errors to be handled on the creator context', async () => {
    const promises = [
      sleep(300).then(() => {
        throw new Error(`Error-1`)
      }),
      sleep(100).then(() => {
        throw new Error(`Error-2`)
      }),
      sleep(500).then(() => {
        throw new Error(`Error-3`)
      }),
    ]

    let hasRecovered = false
    const handleError = jest.fn()
    const recover = async e => {
      hasRecovered = true
      throw e
    }
    const iterator = BufferedAsyncIterator(promises, recover)
    await Promise.all([iterator.next().value, iterator.next().value, iterator.next().value]).catch(
      handleError
    )
    expect(handleError).toHaveBeenCalledWith(new Error('Error-2'))
    expect(hasRecovered).toEqual(true)
  })
})
