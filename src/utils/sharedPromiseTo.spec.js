const sharedPromiseTo = require('./sharedPromiseTo')

describe('Utils > sharedPromiseTo', () => {
  let resolvePromise1, rejectPromise1, sharedPromise1, asyncFunction

  beforeEach(() => {
    asyncFunction = jest.fn(
      () =>
        new Promise((resolve, reject) => {
          resolvePromise1 = resolve
          rejectPromise1 = reject
        })
    )
    sharedPromise1 = sharedPromiseTo(asyncFunction)
  })

  it('Returns the same pending promise for every invocation', async () => {
    const p1 = sharedPromise1()
    const p2 = sharedPromise1()
    const p3 = sharedPromise1()
    expect(Object.is(p1, p2)).toBe(true)
    expect(Object.is(p2, p3)).toBe(true)
    expect(asyncFunction).toHaveBeenCalledTimes(1)
  })

  it('After resolving, returns a new promise on next invocation', async () => {
    const message = 'Resolved promise #1'
    const p1 = sharedPromise1()
    resolvePromise1(message)

    await expect(p1).resolves.toBe(message)
    const p2 = sharedPromise1()
    expect(Object.is(p1, p2)).toBe(false)
    expect(asyncFunction).toHaveBeenCalledTimes(2)
  })

  it('After rejecting, returns a new promise on next invocation', async () => {
    const message = 'Rejected promise #1'
    const p1 = sharedPromise1()
    rejectPromise1(new Error(message))

    await expect(p1).rejects.toThrow(message)
    const p2 = sharedPromise1()
    expect(Object.is(p1, p2)).toBe(false)
    expect(asyncFunction).toHaveBeenCalledTimes(2)
  })

  it('Passes through arguments passed at invocation time', async () => {
    const args = ['arg1', 'arg2']
    const p1 = sharedPromise1(...args)
    resolvePromise1()
    await p1
    expect(asyncFunction).toHaveBeenCalledWith(...args)
  })
})
