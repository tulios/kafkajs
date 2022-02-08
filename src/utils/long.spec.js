const Long = require('./long')

const max = new Long(9223372036854775807n) // max signed int 64

describe('Utils > Long', () => {
  describe('Converters', () => {
    it('fromString(str)', () => {
      const nativeOutput = Long.fromString('9007199254740991')
      expect(nativeOutput).toEqual({ value: 9007199254740991n })
      expect(typeof nativeOutput.value).toEqual('bigint')
    })

    it('toString()', () => {
      const output = new Long(BigInt(10))
      const expectedString = output.toString()
      expect(expectedString).toEqual('10')
      expect(typeof expectedString).toEqual('string')
    })

    it('fromNumber(value)', () => {
      // number
      const numberOutput = Long.fromNumber(12)
      expect(numberOutput).toEqual({ value: 12n })
      expect(typeof numberOutput.value).toEqual('bigint')

      // string
      const stringOutput = Long.fromNumber('12')
      expect(stringOutput).toEqual({ value: 12n })
      expect(typeof stringOutput.value).toEqual('bigint')

      // Long
      const longOutput = new Long(BigInt(12))
      expect(longOutput).toEqual({ value: 12n })
      expect(typeof longOutput.value).toEqual('bigint')
    })

    it('fromValue(value)', () => {
      const output = Long.fromNumber(12)
      expect(output).toEqual({ value: 12n })
      expect(typeof output.value).toEqual('bigint')
    })

    it('fromInt(value)', () => {
      const output = Long.fromInt(12)
      expect(output).toEqual({ value: 12n })
      expect(typeof output.value).toEqual('bigint')
    })

    describe('toInt()', () => {
      it('should return an int', () => {
        const maxInt32 = 2 ** 31 - 1
        const expectedInt = new Long(BigInt(maxInt32)).toInt()
        expect(expectedInt).toEqual(2147483647)
        expect(typeof expectedInt).toEqual('number')
      })

      it('should wrap around if the number is too big to be represented as an int32', () => {
        const maxInt32 = 2 ** 31 - 1
        const expectedInt = new Long(BigInt(maxInt32 + 1)).toInt()
        expect(expectedInt).toEqual(-2147483648)
        expect(typeof expectedInt).toEqual('number')
      })
    })

    it('toNumber()', () => {
      const expectedNumber = max.toNumber()
      expect(expectedNumber).toEqual(9223372036854776000)
      expect(typeof expectedNumber).toEqual('number')
    })

    describe('toJSON()', () => {
      it('should return a string', () => {
        const serialized = max.toJSON()

        expect(serialized).toEqual('9223372036854775807')
      })
    })
  })

  describe('Operators', () => {
    let input1, input2
    beforeAll(() => {
      input1 = new Long(BigInt(5))
      input2 = new Long(BigInt(13))
    })

    describe('Bitwise', () => {
      it('AND', () => {
        const output = input1.and(input2)
        expect(output).toEqual({ value: 5n })
      })

      it('OR', () => {
        const output = input1.or(input2)
        expect(output).toEqual({ value: 13n })
      })

      it('XOR', () => {
        const output = input1.xor(input2)
        expect(output).toEqual({ value: 8n })
      })

      it('NOT', () => {
        const output = input1.not()
        expect(output).toEqual({ value: -6n })
      })

      it('Left shift', () => {
        const output = input1.shiftLeft(1)
        expect(output).toEqual({ value: 10n })
      })

      it('Right shift', () => {
        const output = input1.shiftRight(1)
        expect(output).toEqual({ value: 2n })
      })

      it('Right shift unsigned', () => {
        const output = input1.shiftRightUnsigned(1)
        expect(output).toEqual({ value: 2n })
      })
    })

    describe('Others', () => {
      it('ADD', () => {
        const output = input1.add(input2)
        expect(output).toEqual({ value: 18n })
      })

      it('subtract', () => {
        const output = input1.subtract(input2)
        expect(output).toEqual({ value: -8n })
      })

      it('Equal', () => {
        const expectFalse = input1.equals(input2)
        expect(expectFalse).toEqual(false)

        const expectTrue = input1.equals(input1)
        expect(expectTrue).toEqual(true)
      })

      it('Not equal', () => {
        const expectFalse = input1.notEquals(input2)
        expect(expectFalse).toEqual(true)

        const expectTrue = input1.notEquals(input1)
        expect(expectTrue).toEqual(false)
      })

      it('NEGATE', () => {
        const output = input1.negate()
        expect(output).toEqual({ value: -5n })
      })
    })
  })

  describe('Other functions', () => {
    let input1, input2
    beforeAll(() => {
      input1 = new Long(BigInt(5))
      input2 = new Long(BigInt(13))
    })

    it('getHighBits() & getLowBits()', () => {
      expect(input1.getHighBits()).toEqual(0)
      expect(input1.getLowBits()).toEqual(5)

      expect(input2.getHighBits()).toEqual(0)
      expect(input2.getLowBits()).toEqual(13)

      // 128
      const input = new Long(128n)

      expect(input.getHighBits()).toEqual(0)
      expect(max.getLowBits()).toEqual(-1)
    })

    it('isZero()', () => {
      expect(input1.isZero()).toEqual(false)
      const zero = new Long(BigInt(0))
      expect(zero.isZero()).toEqual(true)
    })

    it('isNegative()', () => {
      expect(new Long(BigInt(-15)).isNegative()).toEqual(true)
      expect(new Long(BigInt(2)).isNegative()).toEqual(false)
    })

    it('multiply()', () => {
      const mult = input1.multiply(input2)
      expect(mult).toEqual({ value: 65n })
      expect(typeof mult.value).toEqual('bigint')
    })

    it('divide()', () => {
      const divide = input2.divide(input1)
      expect(divide).toEqual({ value: 2n })
      expect(typeof divide.value).toEqual('bigint')
    })

    it('compare()', () => {
      expect(input2.compare(input1)).toEqual(1)
      expect(input1.compare(input2)).toEqual(-1)
      expect(input1.compare(input1)).toEqual(0)
    })

    it('lessThan()', () => {
      expect(input2.lessThan(input1)).toEqual(false)
      expect(input1.lessThan(input2)).toEqual(true)
    })

    it('greaterThanOrEqual()', () => {
      expect(input1.greaterThanOrEqual(input2)).toEqual(false)
      expect(input1.greaterThanOrEqual(input1)).toEqual(true)
      expect(input2.greaterThanOrEqual(input1)).toEqual(true)
    })
  })
})
