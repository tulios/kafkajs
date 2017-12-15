const shuffle = require('./shuffle')

describe('Utils > shuffle', () => {
  it('shuffles', () => {
    const array = [1, 2, 3]
    jest
      .spyOn(Math, 'random')
      .mockImplementationOnce(() => 0.4)
      .mockImplementationOnce(() => 0.7)
      .mockImplementationOnce(() => 0.4)
      .mockImplementationOnce(() => 0.6)
      .mockImplementationOnce(() => 0.6)
      .mockImplementationOnce(() => 0.6)

    expect(shuffle(array)).toEqual([1, 3, 2])
    expect(shuffle(array)).toEqual([3, 2, 1])
    expect(Math.random).toHaveBeenCalledTimes(6)
  })
})
