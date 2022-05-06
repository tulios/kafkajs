const once = require('./once')

describe('Utils > once', () => {
  it('should call the wrapped function only once', () => {
    const original = jest.fn().mockReturnValue('foo')
    const wrapped = once(original)

    expect(wrapped('hello')).toEqual('foo')
    expect(wrapped('hello')).toBeUndefined()

    expect(original).toHaveBeenCalledTimes(1)
    expect(original).toHaveBeenCalledWith('hello')
  })
})
