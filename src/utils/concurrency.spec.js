const { KafkaJSNonRetriableError } = require('../errors')
const sleep = require('./sleep')
const concurrency = require('./concurrency')

const after = async (delay, fn) =>
  new Promise(resolve =>
    setTimeout(() => {
      resolve(fn())
    }, delay)
  )

describe('Utils > concurrency', () => {
  it('throws when called with a concurrency of 0 or less', () => {
    expect(() => concurrency()).toThrowWithMessage(
      KafkaJSNonRetriableError,
      '"limit" cannot be less than 1'
    )

    expect(() => concurrency({ limit: 0 })).toThrowWithMessage(
      KafkaJSNonRetriableError,
      '"limit" cannot be less than 1'
    )

    expect(() => concurrency({ limit: NaN })).toThrowWithMessage(
      KafkaJSNonRetriableError,
      '"limit" cannot be less than 1'
    )
  })

  it('invokes functions in sequence with concurrency of 1', async () => {
    const onChange = jest.fn()
    const sequentially = concurrency({ limit: 1, onChange })
    const input = [
      [jest.fn().mockReturnValue(1), 50],
      [jest.fn().mockReturnValue(2), 100],
      [jest.fn().mockReturnValue(3), 10],
      [jest.fn().mockReturnValue(4), 10],
    ]
    const result = await Promise.all(
      input.map(([fn, delay]) => sequentially(async () => after(delay, fn)))
    )

    expect(result).toEqual([1, 2, 3, 4])
    expect(input[0][0]).toHaveBeenCalledBefore(input[1][0])
    expect(input[2][0]).toHaveBeenCalledBefore(input[3][0])
    expect(onChange.mock.calls).toEqual([[1], [0], [1], [0], [1], [0], [1], [0]])
  })

  it('invokes functions concurrently when the limit allows', async () => {
    const onChange = jest.fn()
    const concurrently = concurrency({ limit: 2, onChange })
    const input = [
      [jest.fn().mockReturnValue(1), 50],
      [jest.fn().mockReturnValue(2), 100],
      [jest.fn().mockReturnValue(3), 10],
    ]

    const result = await Promise.all(
      input.map(([fn, delay]) => concurrently(async () => after(delay, fn)))
    )

    expect(result).toEqual([1, 2, 3])
    expect(onChange.mock.calls).toEqual([[1], [2], [1], [2], [1], [0]])
  })

  it('rejects without continuing the chain if there are any errors', async () => {
    const concurrently = concurrency({ limit: 2 })
    const input = [
      [jest.fn(), 1000],
      [jest.fn().mockRejectedValue(new Error('💣')), 500],
      [jest.fn(), 1],
    ]

    const promises = input.map(([fn, delay]) => concurrently(async () => after(delay, fn)))
    await expect(Promise.all(promises)).rejects.toThrow('💣')
    await sleep(1)

    expect(input[2][0]).not.toHaveBeenCalled()
    await expect(promises[2]).rejects.toEqual(
      new KafkaJSNonRetriableError('Queued function aborted due to earlier promise rejection')
    )
  })
})
