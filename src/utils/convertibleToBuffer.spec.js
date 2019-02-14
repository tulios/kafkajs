const convertibleToBuffer = require('./convertibleToBuffer')

class SerializableObject {
  constructor(value) {
    this.value = value
  }
  valueOf() {
    return JSON.stringify(this.value)
  }
}

class ObjectToPrimitive {
  constructor(value) {
    this.value = value
  }

  [Symbol.toPrimitive]() {
    return JSON.stringify(this.value)
  }
}

describe('Utils > convertibleToBuffer', () => {
  const ALLOWED = [
    'strings',
    ['arrays'],
    new SerializableObject('foo'),
    new ObjectToPrimitive('bar'),
    new ArrayBuffer(8),
    Buffer.from([]),
  ]

  ALLOWED.forEach(type => {
    it(`returns true for ${JSON.stringify(type)}`, () => {
      expect(convertibleToBuffer(type)).toBe(true)
    })
  })

  const NOT_ALLOWED = [1, { foo: 'bar' }]

  NOT_ALLOWED.forEach(type => {
    it(`returns false for ${type}`, () => {
      expect(convertibleToBuffer(type)).toBe(false)
    })
  })
})
