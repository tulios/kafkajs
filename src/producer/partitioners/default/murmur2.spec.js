const murmur2 = require('./murmur2')

describe('Producer > Partitioner > Default > murmur2', () => {
  test('it works', () => {
    Object.keys(testData).forEach(key => {
      expect(murmur2(key)).toEqual(testData[key])
    })
  })

  test('it handles numeric input', () => {
    expect(murmur2(0)).toEqual(272173970)
  })

  test('it handles buffer input', () => {
    expect(murmur2(Buffer.from('1'))).toEqual(1311020360)
  })
})

const testData = {
  '0': 272173970,
  '1': 1311020360,
  '128': 2053105854,
  '2187': -2081355488,
  '16384': 204404061,
  '78125': -677491393,
  '279936': -622460209,
  '823543': 651276451,
  '2097152': 944683677,
  '4782969': -892695770,
  '10000000': -1778616326,
  '19487171': -518311627,
  '35831808': 556972389,
  '62748517': -233806557,
  '105413504': -109398538,
  '170859375': 102939717,
}
