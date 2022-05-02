const murmur2 = require('./murmur2')

describe('Producer > Partitioner > Default > murmur2', () => {
  test('it works', () => {
    Object.keys(testData).forEach(key => {
      expect(murmur2(key)).toEqual(testData[key])
    })
  })

  test('it handles numeric input', () => {
    expect(murmur2(0)).toEqual(971027396)
  })

  test('it handles buffer input', () => {
    expect(murmur2(Buffer.from('1'))).toEqual(-1993445489)
  })
})

// Generated with src/producer/partitioners/defaultJava/Test.java
const testData = {
  '0': 971027396,
  '1': -1993445489,
  '128': -326012175,
  '2187': -1508407203,
  '16384': -325739742,
  '78125': -1654490814,
  '279936': 1462227128,
  '823543': -2014198330,
  '2097152': 607668903,
  '4782969': -1182699775,
  '10000000': -1830336757,
  '19487171': -1603849305,
  '35831808': -857013643,
  '62748517': -1167431028,
  '105413504': -381294639,
  '170859375': -1658323481,
  '100:48069': 1009543857,
}
